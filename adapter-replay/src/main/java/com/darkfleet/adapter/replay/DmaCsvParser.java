package com.darkfleet.adapter.replay;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses Danish Maritime Authority (DMA) historical AIS CSV lines into {@link NormalizedAisMessage} records.
 * Every parsed message is explicitly flagged with {@code historical = true}.
 */
@Component
public class DmaCsvParser {

    private static final DateTimeFormatter DMA_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    /**
     * Parses a multi-line DMA CSV payload into normalized messages (position + static Type 5 where present).
     */
    public List<NormalizedAisMessage> parseCsvContent(String csvContent) {
        if (csvContent == null || csvContent.isBlank()) {
            return List.of();
        }
        List<NormalizedAisMessage> results = new ArrayList<>();
        String[] lines = csvContent.split("\\r?\\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.toLowerCase().startsWith("timestamp")) {
                continue;
            }
            results.addAll(parseCsvLine(trimmed));
        }
        return results;
    }

    /**
     * Parses a single DMA CSV row:
     * Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Draught,Destination[,Source]
     */
    public List<NormalizedAisMessage> parseCsvLine(String csvLine) {
        if (csvLine == null || csvLine.isBlank() || csvLine.startsWith("#")) {
            return List.of();
        }
        String[] cols = csvLine.split(",", -1);
        if (cols.length < 10) {
            return List.of();
        }
        try {
            Instant eventTime = parseTimestamp(cols[0].trim());
            String mmsi = cols[2].trim();
            if (mmsi.isEmpty()) {
                return List.of();
            }
            Double lat = parseDoubleOrNull(cols[3]);
            Double lon = parseDoubleOrNull(cols[4]);
            Integer navStatus = parseNaturalNavStatus(cols[5]);
            Double sog = parseDoubleOrDefault(cols[7], 0.0);
            Double cog = parseDoubleOrDefault(cols[8], 0.0);
            Double heading = parseDoubleOrNull(cols[9]);

            String imo = cols.length > 10 ? cleanImo(cols[10]) : null;
            String callSign = cols.length > 11 ? cleanText(cols[11]) : null;
            String vesselName = cols.length > 12 ? cleanText(cols[12]) : null;
            Integer shipType = cols.length > 13 ? parseShipType(cols[13]) : null;
            Double draught = cols.length > 14 ? parseDoubleOrNull(cols[14]) : null;
            String destination = cols.length > 15 ? cleanText(cols[15]) : null;
            AisSource source = cols.length > 16 ? parseSource(cols[16]) : AisSource.DMA_REPLAY;

            List<NormalizedAisMessage> emitted = new ArrayList<>(2);

            if (imo != null || vesselName != null || shipType != null || draught != null) {
                emitted.add(NormalizedAisMessage.staticReport(
                        mmsi,
                        eventTime,
                        source,
                        true,
                        imo,
                        vesselName,
                        callSign,
                        shipType,
                        draught,
                        destination
                ));
            }

            if (lat != null && lon != null) {
                NormalizedAisMessage posMsg = new NormalizedAisMessage(
                        mmsi,
                        eventTime,
                        lat,
                        lon,
                        sog,
                        cog,
                        heading,
                        navStatus,
                        source,
                        AisMessageType.POSITION_REPORT,
                        Instant.now(),
                        true,
                        imo,
                        vesselName,
                        callSign,
                        shipType,
                        draught,
                        destination
                );
                if (posMsg.hasValidCoordinates()) {
                    emitted.add(posMsg);
                }
            }

            return emitted;
        } catch (Exception e) {
            return List.of();
        }
    }

    private Instant parseTimestamp(String raw) {
        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDateTime.parse(raw, DMA_TIMESTAMP_FORMAT).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
            return Instant.now();
        }
    }

    private Double parseDoubleOrNull(String val) {
        if (val == null || val.isBlank() || "Undefined".equalsIgnoreCase(val.trim())) {
            return null;
        }
        try {
            return Double.parseDouble(val.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Double parseDoubleOrDefault(String val, double fallback) {
        Double parsed = parseDoubleOrNull(val);
        return parsed != null ? parsed : fallback;
    }

    private Integer parseNaturalNavStatus(String val) {
        if (val == null || val.isBlank()) {
            return 0;
        }
        String trimmed = val.trim().toLowerCase();
        if (trimmed.contains("under way")) {
            return 0;
        }
        if (trimmed.contains("at anchor")) {
            return 1;
        }
        if (trimmed.contains("moored")) {
            return 5;
        }
        try {
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private Integer parseShipType(String val) {
        if (val == null || val.isBlank()) {
            return null;
        }
        String trimmed = val.trim();
        try {
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException ignored) {
        }
        String lower = trimmed.toLowerCase();
        if (lower.contains("tanker")) {
            return 80;
        }
        if (lower.contains("cargo")) {
            return 70;
        }
        if (lower.contains("tug")) {
            return 52;
        }
        if (lower.contains("pilot")) {
            return 50;
        }
        return null;
    }

    private AisSource parseSource(String val) {
        if (val == null || val.isBlank()) {
            return AisSource.DMA_REPLAY;
        }
        try {
            return AisSource.valueOf(val.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return AisSource.DMA_REPLAY;
        }
    }

    private String cleanImo(String raw) {
        if (raw == null) {
            return null;
        }
        String digits = raw.replaceAll("[^0-9]", "");
        return digits.length() >= 7 ? digits : null;
    }

    private String cleanText(String raw) {
        if (raw == null) {
            return null;
        }
        String cleaned = raw.trim();
        if (cleaned.isEmpty() || "Unknown".equalsIgnoreCase(cleaned) || "Undefined".equalsIgnoreCase(cleaned)) {
            return null;
        }
        return cleaned;
    }
}
