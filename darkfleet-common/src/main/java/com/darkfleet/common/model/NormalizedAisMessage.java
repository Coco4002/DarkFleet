package com.darkfleet.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.Objects;

/**
 * Unified AIS schema produced by all feed adapters (aisstream.io, Fintraffic Digitraffic, DMA Replay).
 * Supports both dynamic position reports and static vessel messages (AIS Type 5: IMO, name, ship type, draught).
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
        String destination
) {

    public NormalizedAisMessage {
        Objects.requireNonNull(mmsi, "mmsi must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(messageType, "messageType must not be null");
        if (ingestionTime == null) {
            ingestionTime = Instant.now();
        }
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
                mmsi,
                eventTime,
                latitude,
                longitude,
                sogKnots,
                cogDegrees,
                headingDegrees,
                navStatus,
                source,
                AisMessageType.POSITION_REPORT,
                Instant.now(),
                historical,
                null,
                null,
                null,
                null,
                null,
                null
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
        return new NormalizedAisMessage(
                mmsi,
                eventTime,
                null,
                null,
                null,
                null,
                null,
                null,
                source,
                AisMessageType.STATIC_VOYAGE,
                Instant.now(),
                historical,
                imoNumber,
                vesselName,
                callSign,
                shipType,
                draughtMeters,
                destination
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
}
