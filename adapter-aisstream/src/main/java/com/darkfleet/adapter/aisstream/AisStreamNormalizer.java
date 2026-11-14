package com.darkfleet.adapter.aisstream;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisEta;
import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/**
 * Turns aisstream.io frames (PositionReport, StandardClassBPositionReport, ShipStaticData) into
 * {@link NormalizedAisMessage}s. A frame without MMSI, time or (for positions) coordinates is dropped;
 * values AIS marks as "not available" become null.
 */
@Component
public class AisStreamNormalizer {

    public Optional<NormalizedAisMessage> normalize(String rawJson) {
        JsonNode root;
        try {
            root = JsonSerde.mapper().readTree(rawJson);
        } catch (JacksonException e) {
            return Optional.empty();
        }
        JsonNode meta = root.path("MetaData");
        Long mmsi = optionalLong(meta, "MMSI");
        Instant time = parseTime(meta.path("time_utc").asString(null));
        if (mmsi == null || time == null) {
            return Optional.empty();
        }
        String type = root.path("MessageType").asString("");
        JsonNode body = root.path("Message").path(type);
        return switch (type) {
            case "PositionReport", "StandardClassBPositionReport" -> position(String.valueOf(mmsi), time, body);
            case "ShipStaticData" -> Optional.of(staticData(String.valueOf(mmsi), time, body));
            default -> Optional.empty();
        };
    }

    private static Optional<NormalizedAisMessage> position(String mmsi, Instant time, JsonNode report) {
        Double lat = optionalDouble(report, "Latitude");
        Double lon = optionalDouble(report, "Longitude");
        if (lat == null || lon == null) {
            return Optional.empty();
        }
        Double sog = optionalDouble(report, "Sog");
        Double cog = optionalDouble(report, "Cog");
        Integer heading = optionalInt(report, "TrueHeading");
        Integer navStatus = optionalInt(report, "NavigationalStatus");
        NormalizedAisMessage msg = new NormalizedAisMessage(
                mmsi, time, lat, lon,
                sog != null && sog < 102.3 ? sog : null,                         // 102.3 = not available
                cog != null && cog < 360 ? cog : null,                           // 360 = not available
                heading != null && heading < 360 ? heading.doubleValue() : null, // 511 = not available
                navStatus != null && navStatus < 15 ? navStatus : null,          // 15 = not defined
                AisSource.AISSTREAM, AisMessageType.POSITION_REPORT, Instant.now(), false,
                null, null, null, null, null, null, null, null, null);
        return msg.hasValidCoordinates() ? Optional.of(msg) : Optional.empty();
    }

    private static NormalizedAisMessage staticData(String mmsi, Instant time, JsonNode data) {
        Long imo = optionalLong(data, "ImoNumber");
        Integer shipType = optionalInt(data, "Type");
        Double draught = optionalDouble(data, "MaximumStaticDraught");
        JsonNode dim = data.path("Dimension");
        JsonNode eta = data.path("Eta");
        Integer month = optionalInt(eta, "Month");
        Integer day = optionalInt(eta, "Day");
        Integer hour = optionalInt(eta, "Hour");
        Integer minute = optionalInt(eta, "Minute");
        return NormalizedAisMessage.staticReport(
                mmsi, time, AisSource.AISSTREAM, false,
                imo != null && imo > 0 ? String.valueOf(imo) : null,  // 0 = no IMO
                clean(data.path("Name").asString(null)),
                clean(data.path("CallSign").asString(null)),
                shipType != null && shipType > 0 ? shipType : null,   // 0 = not available
                draught != null && draught > 0 ? draught : null,      // 0 = not available
                clean(data.path("Destination").asString(null)),
                NormalizedAisMessage.dimension(optionalInt(dim, "A"), optionalInt(dim, "B")),
                NormalizedAisMessage.dimension(optionalInt(dim, "C"), optionalInt(dim, "D")),
                month != null && day != null && hour != null && minute != null
                        ? AisEta.resolve(month, day, hour, minute, time)
                        : null);
    }

    /** aisstream sends e.g. "2026-11-14 16:02:19.455130066 +0000 UTC". */
    static Instant parseTime(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Instant.parse(raw.trim().replace(" +0000 UTC", "Z").replace(' ', 'T'));
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /** AIS pads text with '@'. */
    private static String clean(String value) {
        if (value == null) {
            return null;
        }
        String cleaned = value.replace("@", "").trim();
        return cleaned.isEmpty() ? null : cleaned;
    }

    private static Double optionalDouble(JsonNode node, String field) {
        return node.path(field).isNumber() ? node.get(field).asDouble() : null;
    }

    private static Integer optionalInt(JsonNode node, String field) {
        return node.path(field).isNumber() ? node.get(field).asInt() : null;
    }

    private static Long optionalLong(JsonNode node, String field) {
        return node.path(field).isNumber() ? node.get(field).asLong() : null;
    }
}
