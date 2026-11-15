package com.darkfleet.common.model;

import java.time.Instant;
import java.util.Objects;

/**
 * A vessel's identity and voyage data as broadcast (AIS type 5 / 24) on {@code vessel.static}.
 * Published when the data reported by a feed changes, and repeated daily so the topic always
 * holds every active vessel.
 */
public record VesselStatic(
        String mmsi,
        Instant reportedAt,
        AisSource source,
        String imoNumber,
        String vesselName,
        String callSign,
        Integer shipType,
        Double draughtMeters,
        String destination,
        Integer lengthMeters,
        Integer beamMeters,
        Instant eta
) {

    public VesselStatic {
        Objects.requireNonNull(mmsi, "mmsi");
        Objects.requireNonNull(reportedAt, "reportedAt");
        Objects.requireNonNull(source, "source");
    }

    public static VesselStatic from(NormalizedAisMessage m) {
        return new VesselStatic(m.mmsi(), m.eventTime(), m.source(), m.imoNumber(), m.vesselName(), m.callSign(),
                m.shipType(), m.draughtMeters(), m.destination(), m.lengthMeters(), m.beamMeters(), m.eta());
    }

    /** True when both carry the same reported data, whenever and from wherever it was received. */
    public boolean sameDataAs(VesselStatic other) {
        return other != null
                && Objects.equals(imoNumber, other.imoNumber) && Objects.equals(vesselName, other.vesselName)
                && Objects.equals(callSign, other.callSign) && Objects.equals(shipType, other.shipType)
                && Objects.equals(draughtMeters, other.draughtMeters) && Objects.equals(destination, other.destination)
                && Objects.equals(lengthMeters, other.lengthMeters) && Objects.equals(beamMeters, other.beamMeters)
                && Objects.equals(eta, other.eta);
    }
}
