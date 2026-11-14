package com.darkfleet.adapter.aisstream;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import tools.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/**
 * Parses and normalizes raw aisstream.io JSON frames (PositionReport, StandardClassBPositionReport, ShipStaticData)
 * into DarkFleet's unified {@link NormalizedAisMessage} contract.
 */
@Component
public class AisStreamNormalizer {

    public Optional<NormalizedAisMessage> normalize(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode root = JsonSerde.mapper().readTree(rawJson);
            String messageType = root.path("MessageType").asString("");
            JsonNode meta = root.path("MetaData");
            JsonNode messageNode = root.path("Message");

            String mmsi = extractMmsi(meta, messageNode);
            if (mmsi == null || mmsi.isBlank()) {
                return Optional.empty();
            }

            Instant eventTime = parseTimeUtc(meta.path("time_utc").asString(null));
            String metaShipName = cleanString(meta.path("ShipName").asString(null));

            if ("PositionReport".equalsIgnoreCase(messageType)
                    || "StandardClassBPositionReport".equalsIgnoreCase(messageType)) {
                JsonNode report = messageNode.has("PositionReport")
                        ? messageNode.path("PositionReport")
                        : messageNode.path("StandardClassBPositionReport");

                double lat = report.hasNonNull("Latitude")
                        ? report.path("Latitude").asDouble()
                        : meta.path("latitude").asDouble(Double.NaN);
                double lon = report.hasNonNull("Longitude")
                        ? report.path("Longitude").asDouble()
                        : meta.path("longitude").asDouble(Double.NaN);
                if (Double.isNaN(lat) || Double.isNaN(lon)) {
                    return Optional.empty();
                }

                double sog = report.path("Sog").asDouble(0.0);
                double cog = report.path("Cog").asDouble(0.0);
                Double heading = report.hasNonNull("TrueHeading") && report.path("TrueHeading").asInt(511) != 511
                        ? report.path("TrueHeading").asDouble()
                        : null;
                Integer navStatus = report.hasNonNull("NavigationalStatus")
                        ? report.path("NavigationalStatus").asInt()
                        : null;

                NormalizedAisMessage normalized = new NormalizedAisMessage(
                        mmsi,
                        eventTime,
                        lat,
                        lon,
                        sog,
                        cog,
                        heading,
                        navStatus,
                        AisSource.AISSTREAM,
                        AisMessageType.POSITION_REPORT,
                        Instant.now(),
                        false,
                        null,
                        metaShipName,
                        null,
                        null,
                        null,
                        null
                );
                return normalized.hasValidCoordinates() ? Optional.of(normalized) : Optional.empty();
            } else if ("ShipStaticData".equalsIgnoreCase(messageType)) {
                JsonNode staticData = messageNode.path("ShipStaticData");
                long imoVal = staticData.path("ImoNumber").asLong(0L);
                String imo = imoVal > 0 ? String.valueOf(imoVal) : null;
                String name = cleanString(staticData.path("Name").asString(metaShipName));
                String callSign = cleanString(staticData.path("CallSign").asString(null));
                Integer shipType = staticData.hasNonNull("Type") ? staticData.path("Type").asInt() : null;
                Double draught = staticData.hasNonNull("MaximumStaticDraught")
                        ? staticData.path("MaximumStaticDraught").asDouble()
                        : null;
                String destination = cleanString(staticData.path("Destination").asString(null));

                return Optional.of(NormalizedAisMessage.staticReport(
                        mmsi,
                        eventTime,
                        AisSource.AISSTREAM,
                        false,
                        imo,
                        name,
                        callSign,
                        shipType,
                        draught,
                        destination
                ));
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private String extractMmsi(JsonNode meta, JsonNode messageNode) {
        if (meta.hasNonNull("MMSI")) {
            return String.valueOf(meta.get("MMSI").asLong());
        }
        if (meta.hasNonNull("MMSI_String")) {
            return meta.get("MMSI_String").asString().trim();
        }
        JsonNode pos = messageNode.path("PositionReport");
        if (pos.hasNonNull("UserID")) {
            return String.valueOf(pos.get("UserID").asLong());
        }
        JsonNode stat = messageNode.path("ShipStaticData");
        if (stat.hasNonNull("UserID")) {
            return String.valueOf(stat.get("UserID").asLong());
        }
        return null;
    }

    private Instant parseTimeUtc(String rawTime) {
        if (rawTime == null || rawTime.isBlank()) {
            return Instant.now();
        }
        try {
            return Instant.parse(rawTime.trim());
        } catch (DateTimeParseException ignored) {
        }
        try {
            String normalized = rawTime.trim().replace(" +0000 UTC", "Z").replace(' ', 'T');
            return OffsetDateTime.parse(normalized).toInstant();
        } catch (DateTimeParseException ignored) {
            return Instant.now();
        }
    }

    private String cleanString(String input) {
        if (input == null) {
            return null;
        }
        String cleaned = input.replace("@", "").trim();
        return cleaned.isEmpty() ? null : cleaned;
    }
}
