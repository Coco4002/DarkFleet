package com.darkfleet.common.model;

import java.time.Instant;
import java.util.Objects;

/**
 * One point of a vessel's track on {@code track.points}, merged across feeds and thinned to roughly
 * one point a minute plus turns and speed changes.
 *
 * @param source  the feed that delivered this position
 * @param h3Cell  H3 cell (resolution 6) containing the position
 */
public record TrackPoint(
        String mmsi,
        Instant time,
        double latitude,
        double longitude,
        Double sogKnots,
        Double cogDegrees,
        Double headingDegrees,
        Integer navStatus,
        AisSource source,
        String h3Cell
) {

    public TrackPoint {
        Objects.requireNonNull(mmsi, "mmsi");
        Objects.requireNonNull(time, "time");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(h3Cell, "h3Cell");
    }
}
