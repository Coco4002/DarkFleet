package com.darkfleet.detection;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.CoverageCell;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.common.model.FeedHealth;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.detection.coverage.CellCoverage;
import com.darkfleet.detection.coverage.CoverageProcessor;
import com.darkfleet.detection.coverage.CoverageTick;
import com.darkfleet.detection.encounter.CellProcessor;
import com.darkfleet.detection.encounter.EncounterCandidate;
import com.darkfleet.detection.encounter.PairProcessor;
import com.darkfleet.detection.encounter.Proximity;
import com.darkfleet.detection.identity.IdentityClaim;
import com.darkfleet.detection.identity.IdentityProcessor;
import com.darkfleet.detection.support.Grid;
import com.darkfleet.detection.vessel.VesselProcessor;
import com.darkfleet.detection.vessel.VesselState;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * The Track &amp; Detection topology.
 * <pre>
 *  ais.raw ─┬─ by cell ──► CoverageProcessor ──────────────────────────────► coverage.cells
 *           └─ by MMSI ──► VesselProcessor ─┬─ track points ───────────────► track.points
 *                                           ├─ static data ────────────────► vessel.static
 *                                           ├─ gaps, loitering, identity ──► detections
 *                                           ├─ candidates by cell ► CellProcessor ─ by pair ► PairProcessor ► detections
 *                                           └─ IMO claims by IMO ► IdentityProcessor ──────────────────────► detections
 *  global tables: rules.approved, feed.health, coverage.cells (read back for "is this cell covered")
 * </pre>
 * All state lives in changelogged stores, so a restarted instance picks up where it left off, and
 * the work spreads over up to as many instances as {@code ais.raw} has partitions.
 */
public final class DetectionTopology {

    private static final Serde<String> KEYS = Serdes.String();

    private DetectionTopology() {
    }

    public static KStream<String, NormalizedAisMessage> build(StreamsBuilder builder, DetectionProperties properties) {
        DetectionMode mode = properties.mode();
        DetectionClock clock = new DetectionClock(properties);

        // replay has its own rule topic: a rule approved while replaying must never change live detection
        globalTable(builder, mode.output(DarkFleetTopics.RULES_APPROVED), Lookups.RULES_STORE, DetectionRules.class);
        globalTable(builder, DarkFleetTopics.FEED_HEALTH, Lookups.FEEDS_STORE, FeedHealth.class);
        globalTable(builder, mode.output(DarkFleetTopics.COVERAGE_CELLS), Lookups.CELLS_STORE, CoverageCell.class);

        builder.addStateStore(store(VesselProcessor.STORE, VesselState.class));
        builder.addStateStore(store(CoverageProcessor.STORE, CellCoverage.class));
        builder.addStateStore(store(CellProcessor.STORE, CellProcessor.Candidates.class));
        builder.addStateStore(store(PairProcessor.STORE, PairProcessor.Pair.class));
        builder.addStateStore(store(IdentityProcessor.STORE, IdentityProcessor.Claims.class));

        KStream<String, NormalizedAisMessage> ais = builder.stream(mode.inputTopic(),
                Consumed.with(KEYS, serde(NormalizedAisMessage.class))
                        .withTimestampExtractor(new EventTimeExtractor(properties.maxClockSkew())));

        ais.filter((mmsi, m) -> m != null && m.messageType() == AisMessageType.POSITION_REPORT && m.hasValidCoordinates())
                .map((mmsi, m) -> KeyValue.pair(Grid.cell(m.latitude(), m.longitude()), CoverageTick.of(m)))
                .repartition(Repartitioned.with(KEYS, serde(CoverageTick.class)).withName("coverage-by-cell"))
                .process(() -> new CoverageProcessor(properties, clock), Named.as("coverage"), CoverageProcessor.STORE)
                .to(mode.output(DarkFleetTopics.COVERAGE_CELLS), Produced.with(KEYS, serde(CoverageCell.class)));

        KStream<String, Object> vessels = ais
                .process(() -> new VesselProcessor(properties, clock), Named.as("vessels"), VesselProcessor.STORE);

        only(vessels, TrackPoint.class)
                .to(mode.output(DarkFleetTopics.TRACK_POINTS), Produced.with(KEYS, serde(TrackPoint.class)));
        only(vessels, VesselStatic.class)
                .to(mode.output(DarkFleetTopics.VESSEL_STATIC), Produced.with(KEYS, serde(VesselStatic.class)));
        Produced<String, Detection> detections = Produced.with(KEYS, serde(Detection.class));
        String detectionsTopic = mode.output(DarkFleetTopics.DETECTIONS);
        only(vessels, Detection.class).to(detectionsTopic, detections);

        only(vessels, EncounterCandidate.class)
                .repartition(Repartitioned.with(KEYS, serde(EncounterCandidate.class)).withName("encounter-by-cell"))
                .process(() -> new CellProcessor(properties, clock), Named.as("encounter-cells"), CellProcessor.STORE)
                .repartition(Repartitioned.with(KEYS, serde(Proximity.class)).withName("encounter-by-pair"))
                .process(() -> new PairProcessor(properties, clock), Named.as("encounter-pairs"), PairProcessor.STORE)
                .to(detectionsTopic, detections);

        only(vessels, IdentityClaim.class)
                .repartition(Repartitioned.with(KEYS, serde(IdentityClaim.class)).withName("identity-by-imo"))
                .process(() -> new IdentityProcessor(properties, clock), Named.as("identity"), IdentityProcessor.STORE)
                .to(detectionsTopic, detections);

        return ais;
    }

    private static <T> KStream<String, T> only(KStream<String, Object> stream, Class<T> type) {
        return stream.filter((key, value) -> type.isInstance(value)).mapValues(type::cast);
    }

    private static <T> void globalTable(StreamsBuilder builder, String topic, String storeName, Class<T> type) {
        builder.globalTable(topic, Consumed.with(KEYS, serde(type)),
                Materialized.<String, T>as(Stores.inMemoryKeyValueStore(storeName))
                        .withKeySerde(KEYS).withValueSerde(serde(type)));
    }

    /** In-memory store backed by a changelog topic: rebuilt from Kafka after a restart. */
    private static <T> StoreBuilder<KeyValueStore<String, T>> store(String name, Class<T> type) {
        return Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(name), KEYS, serde(type));
    }

    private static <T> Serde<T> serde(Class<T> type) {
        return new JsonSerde<>(type);
    }
}
