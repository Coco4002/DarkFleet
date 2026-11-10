package com.darkfleet.common.model;

import java.time.Instant;
import java.util.List;

/**
 * Enriched vessel profile maintained by Vessel Intelligence (single source of truth).
 * Links MMSI tracks to IMO-based sanctions records and preserves full static identity history.
 */
public record VesselProfile(
        String mmsi,
        String imoNumber,
        String currentName,
        String callSign,
        Integer shipType,
        String flagState,
        Double currentDraughtMeters,
        CorridorRegion currentCorridor,
        TrackPoint latestPosition,
        boolean sanctioned,
        List<SanctionRecord> sanctionMatches,
        boolean watchlisted,
        String watchlistReason,
        List<VesselStaticReport> identityHistory,
        boolean hasIdentityConflict,
        List<String> gfwEnrichmentNotes,
        Instant updatedAt
) {
}
