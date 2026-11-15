package com.darkfleet.detection.encounter;

import com.darkfleet.detection.DetectionClock;
import com.darkfleet.detection.DetectionProperties;
import com.darkfleet.detection.Lookups;
import com.darkfleet.detection.support.Geo;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Finds encounter candidates close to each other, partitioned by H3 cell. Each cell keeps the latest
 * candidate per vessel for a few minutes; a new candidate is compared with the others in the cell,
 * and pairs within encounter distance go on as a {@link Proximity}, keyed by the pair. A pair is
 * compared only where at least one of the two is actually located, not where both are just copies
 * from neighbouring cells.
 */
public class CellProcessor implements Processor<String, EncounterCandidate, String, Proximity> {

    public static final String STORE = "encounter-cells";
    /** Reports of two vessels this close in time count as "at the same time". */
    static final Duration PAIRING_WINDOW = Duration.ofMinutes(5);

    private final DetectionProperties properties;
    private final DetectionClock clock;
    private ProcessorContext<String, Proximity> context;
    private KeyValueStore<String, Candidates> cells;
    private Lookups lookups;

    public CellProcessor(DetectionProperties properties, DetectionClock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    /** The latest candidate per MMSI in one cell. */
    public static class Candidates {
        public Map<String, EncounterCandidate> byMmsi = new HashMap<>();
    }

    @Override
    public void init(ProcessorContext<String, Proximity> context) {
        this.context = context;
        this.cells = context.getStateStore(STORE);
        this.lookups = new Lookups(context, properties);
        clock.schedule(context, this::forgetOld);
    }

    @Override
    public void process(Record<String, EncounterCandidate> record) {
        EncounterCandidate candidate = record.value();
        if (candidate == null) {
            return;
        }
        Candidates cell = cells.get(record.key());
        if (cell == null) {
            cell = new Candidates();
        }
        double maxDistance = lookups.rules().encounterMaxDistanceMeters();
        for (EncounterCandidate other : cell.byMmsi.values()) {
            if (other.mmsi().equals(candidate.mmsi()) || !(candidate.primary() || other.primary())
                    || Duration.between(other.time(), candidate.time()).abs().compareTo(PAIRING_WINDOW) > 0) {
                continue;
            }
            double distance = Geo.distanceMeters(candidate.latitude(), candidate.longitude(), other.latitude(), other.longitude());
            if (distance <= maxDistance) {
                boolean candidateIsA = candidate.mmsi().compareTo(other.mmsi()) < 0;
                EncounterCandidate a = candidateIsA ? candidate : other;
                EncounterCandidate b = candidateIsA ? other : candidate;
                Instant time = a.time().isAfter(b.time()) ? a.time() : b.time();
                Proximity proximity = new Proximity(a.mmsi(), b.mmsi(), time, distance,
                        (a.latitude() + b.latitude()) / 2, (a.longitude() + b.longitude()) / 2,
                        a.draughtMeters(), b.draughtMeters());
                context.forward(new Record<>(Proximity.pairKey(a.mmsi(), b.mmsi()), proximity, time.toEpochMilli()));
            }
        }
        EncounterCandidate previous = cell.byMmsi.get(candidate.mmsi());
        if (previous == null || candidate.time().isAfter(previous.time())) {
            cell.byMmsi.put(candidate.mmsi(), candidate);
        }
        cell.byMmsi.values().removeIf(c -> c.time().isBefore(candidate.time().minus(PAIRING_WINDOW)));
        cells.put(record.key(), cell);
    }

    private void forgetOld(Instant now) {
        List<KeyValue<String, Candidates>> all = new ArrayList<>();
        try (KeyValueIterator<String, Candidates> iterator = cells.all()) {
            iterator.forEachRemaining(all::add);
        }
        Instant cutoff = now.minus(PAIRING_WINDOW);
        for (KeyValue<String, Candidates> entry : all) {
            if (entry.value.byMmsi.values().removeIf(c -> c.time().isBefore(cutoff))) {
                if (entry.value.byMmsi.isEmpty()) {
                    cells.delete(entry.key);
                } else {
                    cells.put(entry.key, entry.value);
                }
            }
        }
    }
}
