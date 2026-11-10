package com.darkfleet.common.model;

import java.time.Instant;

/**
 * Emitted to ais.candidates.encounter keyed by h3Cell when a cargo or tanker vessel (AIS types 70-89)
 * moves slowly outside port/anchorage exclusion zones, enabling co-partitioned pairwise comparison.
 */
public record EncounterCandidate(
        String h3Cell,
        String mmsi,
        String imoNumber,
        String vesselName,
        Integer shipType,
        double latitude,
        double longitude,
        double sogKnots,
        double cogDegrees,
        Double draughtMeters,
        CorridorRegion corridor,
        AisSource source,
        Instant timestamp,
        boolean historical
) {
}
