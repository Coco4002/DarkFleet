package com.darkfleet.detection.encounter;

import java.time.Instant;

/**
 * Two encounter candidates seen close together, sent to the partition of the pair so that every
 * observation of the same two vessels lands in one place, whichever cell it was seen in.
 * {@code mmsiA} is always the smaller MMSI.
 */
public record Proximity(
        String mmsiA,
        String mmsiB,
        Instant time,
        double distanceMeters,
        double latitude,
        double longitude,
        Double draughtA,
        Double draughtB
) {

    public static String pairKey(String mmsiA, String mmsiB) {
        return mmsiA + "|" + mmsiB;
    }
}
