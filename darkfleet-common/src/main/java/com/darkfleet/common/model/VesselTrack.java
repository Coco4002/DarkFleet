package com.darkfleet.common.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Reconstructed vessel track state emitted by Track & Detection and consumed by Vessel Intelligence and Web App.
 */
public record VesselTrack(
        String mmsi,
        String latestImo,
        String latestVesselName,
        String latestCallSign,
        Integer shipType,
        Double currentDraughtMeters,
        Double previousDraughtMeters,
        String destination,
        CorridorRegion corridor,
        String currentH3Cell,
        TrackPoint latestPosition,
        Map<AisSource, Instant> lastSeenBySource,
        List<TrackPoint> trail,
        List<VesselStaticReport> staticHistory,
        boolean hasIdentityConflict,
        boolean historical,
        Instant updatedAt
) {
}
