package com.darkfleet.adapter.replay;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Parses Danish Maritime Authority AIS CSV files (aisdk-YYYY-MM-DD.csv) into normalized messages.
 * <ul>
 *   <li>Columns are found by header name, so a file with extra or reordered columns still parses.</li>
 *   <li>Only vessels (Class A and Class B) are kept; base stations, aids to navigation and the like are skipped.</li>
 *   <li>A row with a real position becomes a position report. Static rows carry the placeholder
 *       position 91/0 and only yield a static report.</li>
 *   <li>DMA repeats a vessel's static data on many rows; a static report is emitted only when it
 *       differs from the last one emitted for that MMSI.</li>
 *   <li>Every message is marked historical, with source DMA_REPLAY.</li>
 * </ul>
 * One parser instance per file; not thread-safe.
 */
public final class DmaCsvParser {

    private static final DateTimeFormatter DMA_TIME = DateTimeFormatter.ofPattern("dd/MM/uuuu HH:mm:ss", Locale.ROOT);
    private static final int MAX_TRACKED_VESSELS = 500_000;

    private final Map<String, Integer> columns;
    private final Map<String, String> lastStaticSignature = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > MAX_TRACKED_VESSELS;
        }
    };

    private long rows;
    private long skippedNotVessel;
    private long skippedInvalid;

    private DmaCsvParser(Map<String, Integer> columns) {
        this.columns = columns;
    }

    /**
     * @param headerLine the file's first line, e.g. {@code # Timestamp,Type of mobile,MMSI,...}
     * @throws IllegalArgumentException if a required column is missing
     */
    public static DmaCsvParser fromHeader(String headerLine) {
        String header = headerLine.startsWith("#") ? headerLine.substring(1) : headerLine;
        Map<String, Integer> columns = new HashMap<>();
        List<String> names = CsvLine.split(header);
        for (int i = 0; i < names.size(); i++) {
            columns.put(names.get(i).trim().toLowerCase(Locale.ROOT), i);
        }
        for (String required : List.of("timestamp", "type of mobile", "mmsi", "latitude", "longitude")) {
            if (!columns.containsKey(required)) {
                throw new IllegalArgumentException("Not a DMA AIS file: missing column '" + required + "'");
            }
        }
        return new DmaCsvParser(columns);
    }

    /** @return zero, one or two messages (static report first, then position) */
    public List<NormalizedAisMessage> parse(String line) {
        if (line == null || line.isBlank() || line.startsWith("#")) {
            return List.of();
        }
        rows++;
        List<String> f = CsvLine.split(line);

        String mobile = text(f, "type of mobile");
        if (!"Class A".equalsIgnoreCase(mobile) && !"Class B".equalsIgnoreCase(mobile)) {
            skippedNotVessel++;
            return List.of();
        }
        String mmsi = text(f, "mmsi");
        Instant time = timestamp(text(f, "timestamp"));
        if (mmsi == null || !mmsi.matches("\\d{9}") || time == null) {
            skippedInvalid++;
            return List.of();
        }

        List<NormalizedAisMessage> out = new ArrayList<>(2);
        NormalizedAisMessage staticReport = staticReport(f, mmsi, time);
        if (staticReport != null) {
            String signature = staticSignature(staticReport);
            if (!signature.equals(lastStaticSignature.get(mmsi))) {
                lastStaticSignature.put(mmsi, signature);
                out.add(staticReport);
            }
        }
        NormalizedAisMessage position = positionReport(f, mmsi, time);
        if (position != null) {
            out.add(position);
        }
        return out;
    }

    public long rows() {
        return rows;
    }

    public long skippedNotVessel() {
        return skippedNotVessel;
    }

    public long skippedInvalid() {
        return skippedInvalid;
    }

    private NormalizedAisMessage positionReport(List<String> f, String mmsi, Instant time) {
        Double lat = number(f, "latitude");
        Double lon = number(f, "longitude");
        if (lat == null || lon == null) {
            return null;
        }
        Double heading = number(f, "heading");
        if (heading != null && (heading < 0 || heading >= 360)) {
            heading = null; // 511 = not available
        }
        Double cog = number(f, "cog");
        if (cog != null && (cog < 0 || cog >= 360)) {
            cog = null; // 360 = not available
        }
        Double sog = number(f, "sog");
        if (sog != null && (sog < 0 || sog >= 102.3)) {
            sog = null; // 102.3 = not available
        }
        NormalizedAisMessage msg = new NormalizedAisMessage(
                mmsi, time, lat, lon, sog, cog, heading,
                DmaVocabulary.navStatus(text(f, "navigational status")).orElse(null),
                AisSource.DMA_REPLAY, AisMessageType.POSITION_REPORT, Instant.now(), true,
                null, null, null, null, null, null, null, null, null);
        return msg.hasValidCoordinates() ? msg : null;
    }

    private NormalizedAisMessage staticReport(List<String> f, String mmsi, Instant time) {
        String imo = text(f, "imo");
        if (imo != null && !imo.matches("\\d{7}")) {
            imo = null;
        }
        String name = text(f, "name");
        String callSign = text(f, "callsign");
        Integer shipType = DmaVocabulary.shipType(text(f, "ship type")).orElse(null);
        Double draught = number(f, "draught");
        if (draught != null && draught <= 0) {
            draught = null;
        }
        String destination = text(f, "destination");

        Integer length = NormalizedAisMessage.dimension(integer(f, "a"), integer(f, "b"));
        if (length == null) {
            length = positive(integer(f, "length"));
        }
        Integer beam = NormalizedAisMessage.dimension(integer(f, "c"), integer(f, "d"));
        if (beam == null) {
            beam = positive(integer(f, "width"));
        }
        Instant eta = timestamp(text(f, "eta"));

        if (imo == null && name == null && callSign == null && shipType == null && draught == null && length == null) {
            return null;
        }
        return NormalizedAisMessage.staticReport(mmsi, time, AisSource.DMA_REPLAY, true,
                imo, name, callSign, shipType, draught, destination, length, beam, eta);
    }

    private static String staticSignature(NormalizedAisMessage m) {
        return String.join("|",
                Objects.toString(m.imoNumber(), ""), Objects.toString(m.vesselName(), ""),
                Objects.toString(m.callSign(), ""), Objects.toString(m.shipType(), ""),
                Objects.toString(m.draughtMeters(), ""), Objects.toString(m.destination(), ""),
                Objects.toString(m.lengthMeters(), ""), Objects.toString(m.beamMeters(), ""),
                Objects.toString(m.eta(), ""));
    }

    /** Field text, or null when missing, blank or a DMA "unknown" placeholder. */
    private String text(List<String> f, String column) {
        Integer i = columns.get(column);
        if (i == null || i >= f.size()) {
            return null;
        }
        String v = f.get(i).trim();
        if (v.isEmpty() || v.equalsIgnoreCase("Unknown") || v.equalsIgnoreCase("Undefined")
                || v.equalsIgnoreCase("Unknown value")) {
            return null;
        }
        return v;
    }

    private Double number(List<String> f, String column) {
        String v = text(f, column);
        if (v == null) {
            return null;
        }
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Integer integer(List<String> f, String column) {
        Double d = number(f, column);
        return d == null ? null : (int) Math.round(d);
    }

    private static Integer positive(Integer v) {
        return v != null && v > 0 ? v : null;
    }

    private static Instant timestamp(String v) {
        if (v == null) {
            return null;
        }
        try {
            return LocalDateTime.parse(v, DMA_TIME).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
