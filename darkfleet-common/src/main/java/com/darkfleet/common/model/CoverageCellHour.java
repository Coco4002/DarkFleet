package com.darkfleet.common.model;

import java.time.Instant;
import java.util.Map;

/**
 * Coverage statistics per H3 grid cell and 1-hour bucket.
 * Used to suppress AIS gaps in poorly covered waters and verify whether secondary feeds are active.
 */
public record CoverageCellHour(
        String h3Cell,
        Instant hourBucket,
        double centerLat,
        double centerLon,
        CorridorRegion corridor,
        Map<AisSource, Long> messageCountBySource,
        int distinctVessels,
        double coverageScore,
        boolean dualFeedOverlap
) {
}
