package com.darkfleet.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.Objects;

/**
 * Unified AIS schema produced by all feed adapters (aisstream.io, Fintraffic Digitraffic, DMA replay).
 * Carries either a position report or a static vessel message (AIS type 5: IMO, name, ship type,
 * dimensions, draught, destination, ETA).
 *
 * @param lengthMeters overall length (AIS dimension A + B), null if unknown
 * @param beamMeters   overall width (AIS dimension C + D), null if unknown
 * @param eta          estimated time of arrival as reported by the crew, null if not available
 */
public record NormalizedAisMessage(
        String mmsi,
        Instant eventTime,
        Double latitude,
        Double longitude,
        Double sogKnots,
        Double cogDegrees,
        Double headingDegrees,
        Integer navStatus,
        AisSource source,
        AisMessageType messageType,
        Instant ingestionTime,
        boolean historical,
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

    public NormalizedAisMessage {
        Objects.requireNonNull(mmsi, "mmsi must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(messageType, "messageType must not be null");
        Objects.requireNonNull(ingestionTime, "ingestionTime must not be null");
    }

    public static NormalizedAisMessage positionReport(
            String mmsi,
            Instant eventTime,
            double latitude,
            double longitude,
            double sogKnots,
            double cogDegrees,
            Double headingDegrees,
            Integer navStatus,
            AisSource source,
            boolean historical
    ) {
        return new NormalizedAisMessage(
                mmsi, eventTime, latitude, longitude, sogKnots, cogDegrees, headingDegrees, navStatus,
                source, AisMessageType.POSITION_REPORT, Instant.now(), historical,
                null, null, null, null, null, null, null, null, null
        );
    }

    public static NormalizedAisMessage staticReport(
            String mmsi,
            Instant eventTime,
            AisSource source,
            boolean historical,
            String imoNumber,
            String vesselName,
            String callSign,
            Integer shipType,
            Double draughtMeters,
            String destination
    ) {
        return staticReport(mmsi, eventTime, source, historical, imoNumber, vesselName, callSign, shipType,
                draughtMeters, destination, null, null, null);
    }

    public static NormalizedAisMessage staticReport(
            String mmsi,
            Instant eventTime,
            AisSource source,
            boolean historical,
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
        return new NormalizedAisMessage(
                mmsi, eventTime, null, null, null, null, null, null,
                source, AisMessageType.STATIC_VOYAGE, Instant.now(), historical,
                imoNumber, vesselName, callSign, shipType, draughtMeters, destination,
                lengthMeters, beamMeters, eta
        );
    }

    /**
     * AIS ship types 70-79 (Cargo) and 80-89 (Tanker).
     */
    @JsonIgnore
    public boolean isCargoOrTanker() {
        return isCargoOrTankerType(shipType);
    }

    /**
     * AIS ship types 31, 32 (Towing), 50 (Pilot vessel), 52 (Tug).
     */
    @JsonIgnore
    public boolean isTugOrPilot() {
        return isTugOrPilotType(shipType);
    }

    public static boolean isCargoOrTankerType(Integer type) {
        return type != null && type >= 70 && type <= 89;
    }

    public static boolean isTankerType(Integer type) {
        return type != null && type >= 80 && type <= 89;
    }

    public static boolean isTugOrPilotType(Integer type) {
        return type != null && (type == 31 || type == 32 || type == 50 || type == 52);
    }

    @JsonIgnore
    public boolean hasValidCoordinates() {
        return latitude != null && longitude != null
                && latitude >= -90.0 && latitude <= 90.0
                && longitude >= -180.0 && longitude <= 180.0
                && !(Math.abs(latitude) < 0.0001 && Math.abs(longitude) < 0.0001);
    }

    /**
     * Idempotent deduplication key per MMSI, source, messageType, and 1-second timestamp bucket.
     */
    @JsonIgnore
    public String dedupKey() {
        return mmsi + ":" + source.name() + ":" + messageType.name() + ":" + eventTime.getEpochSecond();
    }

    /**
     * The same message with its ingestion time cleared. Two reports of the same transmission
     * received separately are equal by this value; any difference in content makes them unequal.
     */
    @JsonIgnore
    public NormalizedAisMessage withoutIngestionTime() {
        return new NormalizedAisMessage(mmsi, eventTime, latitude, longitude, sogKnots, cogDegrees, headingDegrees,
                navStatus, source, messageType, Instant.EPOCH, historical, imoNumber, vesselName, callSign, shipType,
                draughtMeters, destination, lengthMeters, beamMeters, eta);
    }

    /**
     * Length and beam from the AIS reference-point distances (A to bow, B to stern, C to port,
     * D to starboard). Returns null for a dimension when either side is unknown (0).
     */
    public static Integer dimension(Integer toOneSide, Integer toOtherSide) {
        if (toOneSide == null || toOtherSide == null || toOneSide <= 0 || toOtherSide <= 0) {
            return null;
        }
        return toOneSide + toOtherSide;
    }
}
