package com.darkfleet.common.model;

import java.time.Instant;

/**
 * Dynamic rule parameters governing candidate detection thresholds.
 * Broadcast over the darkfleet.rules Kafka topic when an analyst approves a rule change proposal.
 */
public record DetectionRuleConfig(
        String ruleVersion,
        long minGapMinutes,
        double minCoverageScoreForGap,
        double loiteringMaxSpeedKnots,
        long loiteringMinDurationMinutes,
        double encounterMaxSpeedKnots,
        double encounterMaxDistanceMeters,
        long encounterMinDurationMinutes,
        double draughtChangeMinDeltaMeters,
        double impossibleSpeedThresholdKnots,
        String updatedBy,
        Instant updatedAt
) {

    public static DetectionRuleConfig defaults() {
        return new DetectionRuleConfig(
                "v1.0-default",
                45L,          // minGapMinutes: 45 minutes without AIS position
                0.35,         // minCoverageScoreForGap: suppress gap if cell coverage < 0.35
                3.0,          // loiteringMaxSpeedKnots: <= 3.0 knots offshore
                60L,          // loiteringMinDurationMinutes: >= 60 minutes offshore
                5.0,          // encounterMaxSpeedKnots: slow moving vessels <= 5.0 knots
                600.0,        // encounterMaxDistanceMeters: <= 600 meters apart
                30L,          // encounterMinDurationMinutes: >= 30 minutes side-by-side
                0.4,          // draughtChangeMinDeltaMeters: >= 0.4m draught change after encounter
                42.0,         // impossibleSpeedThresholdKnots: > 42 knots for cargo/tanker
                "system-bootstrap",
                Instant.now()
        );
    }
}
