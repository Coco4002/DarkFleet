package com.darkfleet.common.model;

import java.time.Instant;

public record TrackPoint(
        Instant timestamp,
        double latitude,
        double longitude,
        double sogKnots,
        double cogDegrees,
        Double headingDegrees,
        Integer navStatus,
        AisSource source,
        String h3Cell,
        boolean historical
) {
}
