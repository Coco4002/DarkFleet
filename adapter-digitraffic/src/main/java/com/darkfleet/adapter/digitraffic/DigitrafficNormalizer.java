package com.darkfleet.adapter.digitraffic;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisEta;
import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Optional;

/**
 * Turns Digitraffic MQTT messages (topics vessels-v2/{mmsi}/location and vessels-v2/{mmsi}/metadata)
 * into {@link NormalizedAisMessage}s. A message without time or (for locations) coordinates is
 * dropped; values AIS marks as "not available" become null.
 * Source: Fintraffic / digitraffic.fi, licence CC BY 4.0.
 */
@Component
public class DigitrafficNormalizer {

    public Optional<NormalizedAisMessage> normalizeMqttPayload(String topic, String payloadJson) {
        String[] parts = topic.split("/");
        if (parts.length != 3 || !parts[1].matches("\\d{9}")) {
            return Optional.empty();
        }
        JsonNode node;
        try {
            node = JsonSerde.mapper().readTree(payloadJson);
        } catch (JacksonException e) {
            return Optional.empty();
        }
        return switch (parts[2]) {
            case "location" -> location(parts[1], node);
            case "metadata" -> metadata(parts[1], node);
            default -> Optional.empty();
        };
    }

    /** Location messages carry {@code time} in epoch seconds. */
    private static Optional<NormalizedAisMessage> location(String mmsi, JsonNode node) {
        Long time = optionalLong(node, "time");
        Double lat = optionalDouble(node, "lat");
        Double lon = optionalDouble(node, "lon");
        if (time == null || lat == null || lon == null) {
            return Optional.empty();
        }
        Double sog = optionalDouble(node, "sog");
        Double cog = optionalDouble(node, "cog");
        Integer heading = optionalInt(node, "heading");
        Integer navStatus = optionalInt(node, "navStat");
        NormalizedAisMessage msg = new NormalizedAisMessage(
                mmsi, Instant.ofEpochSecond(time), lat, lon,
                sog != null && sog < 102.3 ? sog : null,                         // 102.3 = not available
                cog != null && cog < 360 ? cog : null,                           // 360 = not available
                heading != null && heading < 360 ? heading.doubleValue() : null, // 511 = not available
                navStatus != null && navStatus < 15 ? navStatus : null,          // 15 = not defined
                AisSource.DIGITRAFFIC, AisMessageType.POSITION_REPORT, Instant.now(), false,
                null, null, null, null, null, null, null, null, null);
        return msg.hasValidCoordinates() ? Optional.of(msg) : Optional.empty();
    }

    /**
     * Metadata messages carry {@code timestamp} in epoch milliseconds, the ship type as {@code type},
     * draught in tenths of a metre, antenna distances refA..refD and the ETA as the packed AIS field.
     */
    private static Optional<NormalizedAisMessage> metadata(String mmsi, JsonNode node) {
        Long timestamp = optionalLong(node, "timestamp");
        if (timestamp == null) {
            return Optional.empty();
        }
        Instant time = Instant.ofEpochMilli(timestamp);
        Long imo = optionalLong(node, "imo");
        Integer shipType = optionalInt(node, "type");
        Integer draughtTenths = optionalInt(node, "draught");
        Long eta = optionalLong(node, "eta");
        return Optional.of(NormalizedAisMessage.staticReport(
                mmsi, time, AisSource.DIGITRAFFIC, false,
                imo != null && imo > 0 ? String.valueOf(imo) : null,                    // 0 = no IMO
                clean(node.path("name").asString(null)),
                clean(node.path("callSign").asString(null)),
                shipType != null && shipType > 0 ? shipType : null,                     // 0 = not available
                draughtTenths != null && draughtTenths > 0 ? draughtTenths / 10.0 : null, // 0 = not available
                clean(node.path("destination").asString(null)),
                NormalizedAisMessage.dimension(optionalInt(node, "refA"), optionalInt(node, "refB")),
                NormalizedAisMessage.dimension(optionalInt(node, "refC"), optionalInt(node, "refD")),
                eta != null ? AisEta.fromPacked(eta, time) : null));
    }

    private static String clean(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
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
