package com.darkfleet.detection;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.CoverageCell;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.common.model.FeedHealth;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.detection.support.Grid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Runs the real topology in Kafka's test driver; time moves only when a test moves it. */
final class Harness implements AutoCloseable {

    static final DetectionRules RULES = new DetectionRules(0, 60, 5, 5, 3.0, 120, 500, 2.0, 120, 24, 0.4);

    final TopologyTestDriver driver;
    final DetectionMode mode;
    private final TestInputTopic<String, NormalizedAisMessage> ais;
    private final TestInputTopic<String, FeedHealth> feeds;
    private final TestInputTopic<String, CoverageCell> cells;
    private final TestInputTopic<String, DetectionRules> rules;
    private final TestOutputTopic<String, Detection> detections;
    private final TestOutputTopic<String, TrackPoint> trackPoints;
    private final TestOutputTopic<String, VesselStatic> statics;
    private final TestOutputTopic<String, CoverageCell> coverage;
    /** Time stamped on the messages a test sends; a test may set it ahead of the wall clock. */
    Instant now;
    private Instant wallClock;

    Harness(DetectionMode mode, Instant start) {
        this(mode, start, Duration.ofHours(3));
    }

    /**
     * @param coverageGrace long by default, so the coverage the topology computes from the test's few
     *                      positions doesn't replace the coverage a test sets up with {@link #cover}
     */
    Harness(DetectionMode mode, Instant start, Duration coverageGrace) {
        this.mode = mode;
        this.now = start;
        this.wallClock = start;
        var properties = new DetectionProperties(mode, Duration.ofMinutes(1), Duration.ofMinutes(2), Duration.ofMinutes(5),
                Duration.ofMinutes(1), 20, 3, Duration.ofHours(24), Duration.ofHours(48), Duration.ofMinutes(30),
                coverageGrace, RULES);
        StreamsBuilder builder = new StreamsBuilder();
        DetectionTopology.build(builder, properties);
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "detection-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        try {
            config.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("detection-test").toString());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        driver = new TopologyTestDriver(builder.build(), config, start);
        var keys = Serdes.String();
        ais = driver.createInputTopic(mode.inputTopic(), keys.serializer(), new JsonSerde<>(NormalizedAisMessage.class).serializer());
        feeds = driver.createInputTopic(DarkFleetTopics.FEED_HEALTH, keys.serializer(), new JsonSerde<>(FeedHealth.class).serializer());
        cells = driver.createInputTopic(mode.output(DarkFleetTopics.COVERAGE_CELLS), keys.serializer(), new JsonSerde<>(CoverageCell.class).serializer());
        rules = driver.createInputTopic(mode.output(DarkFleetTopics.RULES_APPROVED), keys.serializer(), new JsonSerde<>(DetectionRules.class).serializer());
        detections = driver.createOutputTopic(mode.output(DarkFleetTopics.DETECTIONS), keys.deserializer(), new JsonSerde<>(Detection.class).deserializer());
        trackPoints = driver.createOutputTopic(mode.output(DarkFleetTopics.TRACK_POINTS), keys.deserializer(), new JsonSerde<>(TrackPoint.class).deserializer());
        statics = driver.createOutputTopic(mode.output(DarkFleetTopics.VESSEL_STATIC), keys.deserializer(), new JsonSerde<>(VesselStatic.class).deserializer());
        coverage = driver.createOutputTopic(mode.output(DarkFleetTopics.COVERAGE_CELLS), keys.deserializer(), new JsonSerde<>(CoverageCell.class).deserializer());
    }

    // ---- input

    void position(String mmsi, AisSource source, double lat, double lon, Double sog, Double cog) {
        position(mmsi, source, now, lat, lon, sog, cog, 0);
    }

    void position(String mmsi, AisSource source, Instant at, double lat, double lon, Double sog, Double cog, Integer navStatus) {
        ais.pipeInput(mmsi, new NormalizedAisMessage(mmsi, at, lat, lon, sog, cog, null, navStatus, source,
                com.darkfleet.common.model.AisMessageType.POSITION_REPORT, at, mode == DetectionMode.REPLAY,
                null, null, null, null, null, null, null, null, null), at);
    }

    void staticData(String mmsi, AisSource source, String imo, String name, Integer shipType, Double draught) {
        ais.pipeInput(mmsi, NormalizedAisMessage.staticReport(mmsi, now, source, mode == DetectionMode.REPLAY,
                imo, name, null, shipType, draught, null), now);
    }

    void tanker(String mmsi, AisSource source, String imo, Double draught) {
        staticData(mmsi, source, imo, "TANKER " + mmsi, 80, draught);
    }

    void raw(NormalizedAisMessage message) {
        ais.pipeInput(message.mmsi(), message, message.eventTime());
    }

    /** A heartbeat saying the feed is connected and delivering right now. */
    void feedWorking(AisSource source) {
        feeds.pipeInput(source.name(), new FeedHealth(source, now, 30, true, 100, 90, now, 1000, 0, 0), now);
    }

    /** Coverage history making the cell count as well covered by both live feeds. */
    void cover(double lat, double lon) {
        coverAt(lat, lon, now.truncatedTo(ChronoUnit.HOURS).minus(Duration.ofHours(1)));
    }

    void approve(DetectionRules approved) {
        rules.pipeInput(DetectionRules.KEY, approved, now);
    }

    /** Moves the wall clock to {@code now + duration}, a minute at a time, the given feeds sending heartbeats. */
    void advance(Duration duration, AisSource... workingFeeds) {
        Instant target = now.plus(duration);
        while (wallClock.isBefore(target)) {
            wallClock = wallClock.plus(Duration.ofMinutes(1));
            now = wallClock;
            for (AisSource feed : workingFeeds) {
                feedWorking(feed);
            }
            driver.advanceWallClockTime(Duration.ofMinutes(1));
        }
    }

    /** Coverage history of a cell as published at {@code hourStart}. */
    void coverAt(double lat, double lon, Instant hourStart) {
        String cell = Grid.cell(lat, lon);
        Map<AisSource, Long> plenty = Map.of(AisSource.AISSTREAM, 500L, AisSource.DIGITRAFFIC, 500L, AisSource.DMA_REPLAY, 500L);
        cells.pipeInput(cell, new CoverageCell(cell, hourStart, plenty, 20, 0, plenty, 0), now);
    }

    // ---- output

    List<Detection> detections() {
        return detections.readValuesToList();
    }

    List<TrackPoint> trackPoints() {
        return trackPoints.readValuesToList();
    }

    List<VesselStatic> statics() {
        return statics.readValuesToList();
    }

    List<CoverageCell> coverage() {
        return coverage.readValuesToList();
    }

    @Override
    public void close() {
        driver.close();
    }
}
