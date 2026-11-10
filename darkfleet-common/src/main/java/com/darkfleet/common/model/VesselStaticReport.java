package com.darkfleet.common.model;

import java.time.Instant;

/**
 * Immutable record of a static AIS Type 5 report for a vessel.
 * Conflicting reports (e.g. changing name, IMO, shipType, or draught) are appended to history, never overwritten.
 */
public record VesselStaticReport(
        String mmsi,
        String imoNumber,
        String vesselName,
        String callSign,
        Integer shipType,
        Double draughtMeters,
        String destination,
        AisSource source,
        Instant reportedAt,
        boolean historical
) {
}
