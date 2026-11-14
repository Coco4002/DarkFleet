package com.darkfleet.adapter.replay;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * DMA files spell out ship types and navigational statuses as text; these map them back to the
 * AIS codes the rest of the system uses. Unknown, undefined and reserved values map to empty.
 */
final class DmaVocabulary {

    private static final Map<String, Integer> SHIP_TYPES = Map.ofEntries(
            Map.entry("wig", 20),
            Map.entry("fishing", 30),
            Map.entry("towing", 31),
            Map.entry("towing long/wide", 32),
            Map.entry("dredging", 33),
            Map.entry("diving", 34),
            Map.entry("military", 35),
            Map.entry("sailing", 36),
            Map.entry("pleasure", 37),
            Map.entry("hsc", 40),
            Map.entry("pilot", 50),
            Map.entry("sar", 51),
            Map.entry("tug", 52),
            Map.entry("port tender", 53),
            Map.entry("anti-pollution", 54),
            Map.entry("law enforcement", 55),
            Map.entry("medical", 58),
            Map.entry("not party to conflict", 59),
            Map.entry("passenger", 60),
            Map.entry("cargo", 70),
            Map.entry("tanker", 80),
            Map.entry("other", 90)
    );

    private static final Map<String, Integer> NAV_STATUSES = Map.ofEntries(
            Map.entry("under way using engine", 0),
            Map.entry("at anchor", 1),
            Map.entry("not under command", 2),
            Map.entry("restricted maneuverability", 3),
            Map.entry("constrained by her draught", 4),
            Map.entry("moored", 5),
            Map.entry("aground", 6),
            Map.entry("engaged in fishing", 7),
            Map.entry("under way sailing", 8),
            Map.entry("reserved for future amendment [hsc]", 9),
            Map.entry("reserved for future amendment [wig]", 10),
            Map.entry("power-driven vessel towing astern", 11),
            Map.entry("power-driven vessel pushing ahead or towing alongside", 12),
            Map.entry("ais-sart", 14)
    );

    private DmaVocabulary() {
    }

    static Optional<Integer> shipType(String text) {
        return Optional.ofNullable(SHIP_TYPES.get(normalize(text)));
    }

    static Optional<Integer> navStatus(String text) {
        return Optional.ofNullable(NAV_STATUSES.get(normalize(text)));
    }

    private static String normalize(String text) {
        return text == null ? "" : text.trim().toLowerCase(Locale.ROOT);
    }
}
