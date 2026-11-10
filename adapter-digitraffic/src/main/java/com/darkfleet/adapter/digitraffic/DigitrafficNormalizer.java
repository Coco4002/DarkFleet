package com.darkfleet.adapter.digitraffic;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

/**
 * Normalizes Fintraffic Digitraffic AIS payloads (MQTT vessels-v2/+/location, vessels-v2/+/metadata,
 * and REST GeoJSON Feature location payloads).
 * Credit: Fintraffic / digitraffic.fi (CC BY 4.0).
 */
@Component
public class DigitrafficNormalizer {

    public Optional<NormalizedAisMessage> normalizeMqttPayload(String mqttTopic, String payloadJson) {
        if (mqttTopic == null || payloadJson == null || payloadJson.isBlank()) {
            return Optional.empty();
        }
        String[] parts = mqttTopic.split("/");
        if (parts.length < 3) {
            return Optional.empty();
        }
        String mmsi = parts[1].trim();
        String messageCategory = parts[2].trim();

        try {
            JsonNode root = JsonSerde.mapper().readTree(payloadJson);
            if ("location".equalsIgnoreCase(messageCategory)) {
                return parseLocationNode(mmsi, root);
            } else if ("metadata".equalsIgnoreCase(messageCategory)) {
                return parseMetadataNode(mmsi, root);
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public Optional<NormalizedAisMessage> normalizeGeoJsonFeature(JsonNode feature) {
        if (feature == null || !feature.isObject()) {
            return Optional.empty();
        }
        try {
            String mmsi = feature.hasNonNull("mmsi")
                    ? String.valueOf(feature.get("mmsi").asLong())
                    : feature.path("properties").path("mmsi").asText(null);
            if (mmsi == null || mmsi.isBlank()) {
                return Optional.empty();
            }
            JsonNode coords = feature.path("geometry").path("coordinates");
            if (!coords.isArray() || coords.size() < 2) {
                return Optional.empty();
            }
            double lon = coords.get(0).asDouble();
            double lat = coords.get(1).asDouble();

            JsonNode props = feature.path("properties");
            long tsMillis = props.path("timestampExternal").asLong(System.currentTimeMillis());
            Instant eventTime = Instant.ofEpochMilli(tsMillis);

            double sog = props.path("sog").asDouble(0.0);
            double cog = props.path("cog").asDouble(0.0);
            Double heading = props.hasNonNull("heading") && props.path("heading").asInt(511) != 511
                    ? props.path("heading").asDouble()
                    : null;
            Integer navStat = props.hasNonNull("navStat") ? props.path("navStat").asInt() : null;

            NormalizedAisMessage msg = NormalizedAisMessage.positionReport(
                    mmsi,
                    eventTime,
                    lat,
                    lon,
                    sog,
                    cog,
                    heading,
                    navStat,
                    AisSource.DIGITRAFFIC,
                    false
            );
            return msg.hasValidCoordinates() ? Optional.of(msg) : Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Optional<NormalizedAisMessage> parseLocationNode(String mmsi, JsonNode node) {
        if (!node.hasNonNull("lat") || !node.hasNonNull("lon")) {
            return Optional.empty();
        }
        double lat = node.path("lat").asDouble();
        double lon = node.path("lon").asDouble();
        long timeValue = node.path("time").asLong(0L);
        Instant eventTime = timeValue > 100_000_000_000L
                ? Instant.ofEpochMilli(timeValue)
                : (timeValue > 0 ? Instant.ofEpochSecond(timeValue) : Instant.now());

        double sog = node.path("sog").asDouble(0.0);
        double cog = node.path("cog").asDouble(0.0);
        Double heading = node.hasNonNull("heading") && node.path("heading").asInt(511) != 511
                ? node.path("heading").asDouble()
                : null;
        Integer navStat = node.hasNonNull("navStat") ? node.path("navStat").asInt() : null;

        NormalizedAisMessage msg = new NormalizedAisMessage(
                mmsi,
                eventTime,
                lat,
                lon,
                sog,
                cog,
                heading,
                navStat,
                AisSource.DIGITRAFFIC,
                AisMessageType.POSITION_REPORT,
                Instant.now(),
                false,
                null,
                null,
                null,
                null,
                null,
                null
        );
        return msg.hasValidCoordinates() ? Optional.of(msg) : Optional.empty();
    }

    private Optional<NormalizedAisMessage> parseMetadataNode(String mmsi, JsonNode node) {
        long ts = node.path("timestamp").asLong(0L);
        Instant eventTime = ts > 100_000_000_000L
                ? Instant.ofEpochMilli(ts)
                : (ts > 0 ? Instant.ofEpochSecond(ts) : Instant.now());

        long imoRaw = node.path("imo").asLong(0L);
        String imo = imoRaw > 0 ? String.valueOf(imoRaw) : null;
        String name = clean(node.path("name").asText(null));
        String callSign = clean(node.path("callSign").asText(null));
        Integer shipType = node.hasNonNull("shipType") ? node.path("shipType").asInt() : null;
        Double draught = null;
        if (node.hasNonNull("draught")) {
            double rawDraught = node.path("draught").asDouble();
            // Digitraffic encodes draught in 1/10th meters (e.g. 125 -> 12.5m)
            draught = rawDraught > 35.0 ? rawDraught / 10.0 : rawDraught;
        }
        String destination = clean(node.path("destination").asText(null));

        return Optional.of(NormalizedAisMessage.staticReport(
                mmsi,
                eventTime,
                AisSource.DIGITRAFFIC,
                false,
                imo,
                name,
                callSign,
                shipType,
                draught,
                destination
        ));
    }

    private String clean(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
