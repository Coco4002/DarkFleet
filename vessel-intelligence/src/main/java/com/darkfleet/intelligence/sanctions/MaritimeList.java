package com.darkfleet.intelligence.sanctions;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads OpenSanctions' maritime CSV ({@code type, caption, imo, risk, countries, flag, mmsi, id, url,
 * datasets, aliases}). Keeps vessels that carry at least one risk topic and can be matched to AIS data
 * (an IMO number or an MMSI); organizations and unmatchable rows are counted as skipped.
 */
final class MaritimeList {

    static final List<String> REQUIRED_COLUMNS =
            List.of("type", "caption", "imo", "risk", "countries", "flag", "mmsi", "id", "url", "datasets", "aliases");
    private static final Pattern IMO = Pattern.compile("(?i)^IMO(\\d{7})$");
    private static final Pattern MMSI = Pattern.compile("^\\d{9}$");

    record Listing(String id, String name, String imo, List<String> mmsi, List<String> topics, String flag,
                   List<String> countries, List<String> datasets, List<String> aliases, String url) {
    }

    record Parsed(List<Listing> listings, int skipped) {
    }

    private MaritimeList() {
    }

    static Parsed parse(Reader reader) throws IOException {
        Csv csv = new Csv(reader);
        List<String> header = csv.next();
        if (header == null) {
            throw new IOException("The file is empty");
        }
        Map<String, Integer> columns = new HashMap<>();
        for (int i = 0; i < header.size(); i++) {
            columns.put(header.get(i).strip().replace("﻿", ""), i);
        }
        List<String> missing = REQUIRED_COLUMNS.stream().filter(c -> !columns.containsKey(c)).toList();
        if (!missing.isEmpty()) {
            throw new IOException("The file lacks the columns " + missing + "; the format may have changed");
        }

        List<Listing> listings = new ArrayList<>();
        int skipped = 0;
        for (List<String> next = csv.next(); next != null; next = csv.next()) {
            List<String> row = next;
            if (row.size() == 1 && row.getFirst().isEmpty()) {
                continue; // blank line
            }
            if (row.size() < header.size()) {
                throw new IOException("A record has " + row.size() + " fields, the header " + header.size());
            }
            Field f = name -> row.get(columns.get(name)).strip();
            List<String> topics = split(f.get("risk"));
            String imo = imo(f.get("imo"));
            List<String> mmsi = split(f.get("mmsi")).stream().filter(m -> MMSI.matcher(m).matches()).toList();
            if (!"VESSEL".equals(f.get("type")) || topics.isEmpty() || (imo == null && mmsi.isEmpty())
                    || f.get("id").isEmpty()) {
                skipped++;
                continue;
            }
            listings.add(new Listing(f.get("id"), f.get("caption"), imo, mmsi, topics, emptyToNull(f.get("flag")),
                    split(f.get("countries")), split(f.get("datasets")), split(f.get("aliases")), f.get("url")));
        }
        return new Parsed(listings, skipped);
    }

    private interface Field {
        String get(String name);
    }

    private static String imo(String value) {
        Matcher m = IMO.matcher(value);
        return m.matches() ? m.group(1) : null;
    }

    private static List<String> split(String value) {
        if (value.isEmpty()) {
            return List.of();
        }
        return Arrays.stream(value.split(";")).map(String::strip).filter(s -> !s.isEmpty()).toList();
    }

    private static String emptyToNull(String value) {
        return value.isEmpty() ? null : value;
    }
}
