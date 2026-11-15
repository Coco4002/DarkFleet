package com.darkfleet.detection.identity;

import com.darkfleet.common.model.AisSource;

import java.time.Instant;

/** "MMSI {@code mmsi} broadcast IMO {@code imo} at {@code time}", sent to the IMO's partition. */
public record IdentityClaim(
        String imo,
        String mmsi,
        String vesselName,
        Instant time,
        AisSource source
) {
}
