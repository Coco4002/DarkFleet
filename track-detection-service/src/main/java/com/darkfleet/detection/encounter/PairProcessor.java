package com.darkfleet.detection.encounter;

import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.detection.DetectionClock;
import com.darkfleet.detection.DetectionProperties;
import com.darkfleet.detection.Lookups;
import com.darkfleet.detection.support.Evidence;
import com.darkfleet.detection.support.Grid;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns proximity observations of one pair of vessels into a potential encounter, partitioned by
 * the pair. An encounter opens once the two have been observed together for the encounter time
 * without a break longer than the episode break, and closes after such a break. The draught each
 * vessel reported at the start and at the end goes into the evidence: a laden tanker meeting an
 * empty one and both changing draught is what a transfer would look like.
 */
public class PairProcessor implements Processor<String, Proximity, String, Detection> {

    public static final String STORE = "encounter-pairs";

    private final DetectionProperties properties;
    private final DetectionClock clock;
    private ProcessorContext<String, Detection> context;
    private KeyValueStore<String, Pair> pairs;
    private Lookups lookups;

    public PairProcessor(DetectionProperties properties, DetectionClock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    public static class Pair {
        public Instant startedAt;
        public Instant lastAt;
        public double startLatitude;
        public double startLongitude;
        public int observations;
        public double minDistanceMeters;
        public Double draughtAStart;
        public Double draughtAEnd;
        public Double draughtBStart;
        public Double draughtBEnd;
        public Detection opened;
    }

    @Override
    public void init(ProcessorContext<String, Detection> context) {
        this.context = context;
        this.pairs = context.getStateStore(STORE);
        this.lookups = new Lookups(context, properties);
        clock.schedule(context, this::endQuietPairs);
    }

    @Override
    public void process(Record<String, Proximity> record) {
        Proximity p = record.value();
        if (p == null) {
            return;
        }
        Pair pair = pairs.get(record.key());
        if (pair != null && !p.time().isAfter(pair.lastAt)) {
            return; // the same observation from a neighbouring cell, or an older one
        }
        if (pair != null && Duration.between(pair.lastAt, p.time()).compareTo(properties.episodeBreak()) > 0) {
            end(pair);
            pair = null;
        }
        if (pair == null) {
            pair = new Pair();
            pair.startedAt = p.time();
            pair.startLatitude = p.latitude();
            pair.startLongitude = p.longitude();
            pair.minDistanceMeters = p.distanceMeters();
        }
        pair.lastAt = p.time();
        pair.observations++;
        pair.minDistanceMeters = Math.min(pair.minDistanceMeters, p.distanceMeters());
        if (p.draughtA() != null) {
            pair.draughtAStart = pair.draughtAStart == null ? p.draughtA() : pair.draughtAStart;
            pair.draughtAEnd = p.draughtA();
        }
        if (p.draughtB() != null) {
            pair.draughtBStart = pair.draughtBStart == null ? p.draughtB() : pair.draughtBStart;
            pair.draughtBEnd = p.draughtB();
        }
        DetectionRules rules = lookups.rules();
        if (pair.opened == null
                && Duration.between(pair.startedAt, pair.lastAt).toMinutes() >= rules.encounterMinMinutes()) {
            pair.opened = new Detection("encounter:" + p.mmsiA() + ":" + p.mmsiB() + ":" + pair.startedAt.getEpochSecond(),
                    DetectionType.POTENTIAL_ENCOUNTER, DetectionStatus.OPEN, 1, p.mmsiA(), p.mmsiB(), pair.startedAt, null,
                    pair.startLatitude, pair.startLongitude, Grid.cell(pair.startLatitude, pair.startLongitude),
                    rules.version(), Evidence.of(
                            "maxDistanceMeters", rules.encounterMaxDistanceMeters(),
                            "maxSpeedKnots", rules.encounterMaxSpeedKnots(),
                            "minMinutes", rules.encounterMinMinutes(),
                            "draughtAStartMeters", pair.draughtAStart, "draughtBStartMeters", pair.draughtBStart));
            context.forward(new Record<>(p.mmsiA(), pair.opened, p.time().toEpochMilli()));
        }
        pairs.put(record.key(), pair);
    }

    private void endQuietPairs(Instant now) {
        List<KeyValue<String, Pair>> all = new ArrayList<>();
        try (KeyValueIterator<String, Pair> iterator = pairs.all()) {
            iterator.forEachRemaining(all::add);
        }
        for (KeyValue<String, Pair> entry : all) {
            if (Duration.between(entry.value.lastAt, now).compareTo(properties.episodeBreak()) > 0) {
                end(entry.value);
                pairs.delete(entry.key);
            }
        }
    }

    private void end(Pair pair) {
        if (pair.opened == null) {
            return;
        }
        double threshold = lookups.rules().draughtChangeMeters();
        Double changeA = change(pair.draughtAStart, pair.draughtAEnd);
        Double changeB = change(pair.draughtBStart, pair.draughtBEnd);
        Detection closed = pair.opened.closed(pair.lastAt, Evidence.of(
                "closeReason", "apart or no longer slow",
                "durationMinutes", Duration.between(pair.startedAt, pair.lastAt).toMinutes(),
                "observations", pair.observations,
                "minDistanceMeters", Evidence.round(pair.minDistanceMeters, 0),
                "draughtAEndMeters", pair.draughtAEnd, "draughtBEndMeters", pair.draughtBEnd,
                "draughtChangeAMeters", changeA, "draughtChangeBMeters", changeB,
                "draughtChanged", changeA != null && Math.abs(changeA) >= threshold
                        || changeB != null && Math.abs(changeB) >= threshold));
        context.forward(new Record<>(pair.opened.mmsi(), closed, pair.lastAt.toEpochMilli()));
    }

    private static Double change(Double start, Double end) {
        return start == null || end == null ? null : Evidence.round(end - start, 1);
    }
}
