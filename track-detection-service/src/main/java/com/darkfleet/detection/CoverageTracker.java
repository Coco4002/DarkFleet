package com.darkfleet.detection;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.CorridorRegion;
import com.darkfleet.common.model.CoverageCellHour;
import com.darkfleet.common.spatial.SpatialEngine;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains derived streaming coverage statistics per H3 grid cell and 1-hour bucket across feeds.
 * Used to suppress AIS gaps in poorly covered waters and identify dual-feed overlap cells.
 */
@Component
public class CoverageTracker {

    private final Map<String, CellHourAccumulator> store = new ConcurrentHashMap<>();

    public CoverageCellHour recordPosition(
            String h3Cell,
            double lat,
            double lon,
            Instant eventTime,
            AisSource source,
            String mmsi
    ) {
        Instant hourBucket = eventTime.truncatedTo(ChronoUnit.HOURS);
        String key = cellHourKey(h3Cell, hourBucket);
        CellHourAccumulator acc = store.computeIfAbsent(
                key,
                k -> new CellHourAccumulator(h3Cell, hourBucket, CorridorRegion.classify(lat, lon))
        );
        acc.record(source, mmsi);
        return acc.toSnapshot();
    }

    /**
     * Returns the measured coverage score [0.0, 1.0] for an H3 cell at the given timestamp
     * (checking both the exact hour bucket and the preceding hour bucket).
     */
    public double getCellCoverageScore(String h3Cell, Instant timestamp) {
        Instant currentHour = timestamp.truncatedTo(ChronoUnit.HOURS);
        Instant prevHour = currentHour.minus(1, ChronoUnit.HOURS);

        CellHourAccumulator currentAcc = store.get(cellHourKey(h3Cell, currentHour));
        CellHourAccumulator prevAcc = store.get(cellHourKey(h3Cell, prevHour));

        double currentScore = currentAcc != null ? currentAcc.computeCoverageScore() : 0.0;
        double prevScore = prevAcc != null ? prevAcc.computeCoverageScore() : 0.0;
        return Math.max(currentScore, prevScore);
    }

    public CoverageCellHour getCellCoverage(String h3Cell, Instant timestamp) {
        Instant hourBucket = timestamp.truncatedTo(ChronoUnit.HOURS);
        CellHourAccumulator acc = store.get(cellHourKey(h3Cell, hourBucket));
        if (acc != null) {
            return acc.toSnapshot();
        }
        double[] center = SpatialEngine.cellCenter(h3Cell);
        return new CoverageCellHour(
                h3Cell,
                hourBucket,
                center[0],
                center[1],
                CorridorRegion.classify(center[0], center[1]),
                Map.of(),
                0,
                0.0,
                false
        );
    }

    public List<CoverageCellHour> listAllCoverageCells() {
        return store.values().stream()
                .map(CellHourAccumulator::toSnapshot)
                .sorted(Comparator.comparing(CoverageCellHour::hourBucket).reversed()
                        .thenComparing(CoverageCellHour::h3Cell))
                .toList();
    }

    public void clear() {
        store.clear();
    }

    private static String cellHourKey(String h3Cell, Instant hourBucket) {
        return h3Cell + "@" + hourBucket.toString();
    }

    private static final class CellHourAccumulator {
        private final String h3Cell;
        private final Instant hourBucket;
        private final CorridorRegion corridor;
        private final Map<AisSource, AtomicLong> countsBySource = new ConcurrentHashMap<>();
        private final Set<String> distinctVessels = ConcurrentHashMap.newKeySet();

        private CellHourAccumulator(String h3Cell, Instant hourBucket, CorridorRegion corridor) {
            this.h3Cell = h3Cell;
            this.hourBucket = hourBucket;
            this.corridor = corridor;
        }

        void record(AisSource source, String mmsi) {
            countsBySource.computeIfAbsent(source, s -> new AtomicLong(0)).incrementAndGet();
            distinctVessels.add(mmsi);
        }

        double computeCoverageScore() {
            long totalMessages = countsBySource.values().stream().mapToLong(AtomicLong::get).sum();
            int vessels = distinctVessels.size();
            boolean dualFeed = countsBySource.getOrDefault(AisSource.AISSTREAM, new AtomicLong(0)).get() > 0
                    && countsBySource.getOrDefault(AisSource.DIGITRAFFIC, new AtomicLong(0)).get() > 0;

            double messageFactor = Math.min(1.0, totalMessages / 15.0);
            double vesselFactor = Math.min(1.0, vessels / 4.0);
            double raw = (0.55 * messageFactor) + (0.35 * vesselFactor) + (dualFeed ? 0.10 : 0.0);
            return Math.min(1.0, Math.round(raw * 100.0) / 100.0);
        }

        CoverageCellHour toSnapshot() {
            double[] center = SpatialEngine.cellCenter(h3Cell);
            Map<AisSource, Long> snapshotCounts = new EnumMap<>(AisSource.class);
            countsBySource.forEach((src, cnt) -> snapshotCounts.put(src, cnt.get()));
            boolean dualFeed = snapshotCounts.getOrDefault(AisSource.AISSTREAM, 0L) > 0
                    && snapshotCounts.getOrDefault(AisSource.DIGITRAFFIC, 0L) > 0;
            return new CoverageCellHour(
                    h3Cell,
                    hourBucket,
                    center[0],
                    center[1],
                    corridor,
                    snapshotCounts,
                    distinctVessels.size(),
                    computeCoverageScore(),
                    dualFeed
            );
        }
    }
}
