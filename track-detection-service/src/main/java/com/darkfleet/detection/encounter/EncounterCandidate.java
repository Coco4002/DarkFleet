package com.darkfleet.detection.encounter;

import java.time.Instant;

/**
 * A slow cargo vessel or tanker away from ports, sent to the cell it is in ({@code primary}) and to
 * any neighbouring cell within encounter distance, so two vessels on either side of a cell border
 * still meet in one place.
 */
public record EncounterCandidate(
        String mmsi,
        Instant time,
        double latitude,
        double longitude,
        double sogKnots,
        Double draughtMeters,
        boolean primary
) {
}
