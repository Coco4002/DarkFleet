package com.darkfleet.intelligence.sanctions;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MaritimeListTest {

    @Test
    void keepsMatchableVesselsWithARiskTopic() throws IOException {
        MaritimeList.Parsed parsed;
        try (var reader = new InputStreamReader(
                getClass().getResourceAsStream("/sanctions/maritime-sample.csv"), StandardCharsets.UTF_8)) {
            parsed = MaritimeList.parse(reader);
        }
        Map<String, MaritimeList.Listing> byId = parsed.listings().stream()
                .collect(Collectors.toMap(MaritimeList.Listing::id, Function.identity()));

        assertEquals(List.of("eu-fsf-eventin", "shadow-runner", "detained-one", "warned"),
                parsed.listings().stream().map(MaritimeList.Listing::id).toList());
        // no risk topic, no identifier, not a vessel
        assertEquals(3, parsed.skipped());

        MaritimeList.Listing eventin = byId.get("eu-fsf-eventin");
        assertEquals("9308065", eventin.imo(), "IMO prefix removed");
        assertEquals(List.of("sanction", "mare.shadow"), eventin.topics());
        assertEquals(List.of("eu_fsf", "gb_hmt_sanctions"), eventin.datasets());
        assertEquals(List.of("EVENTIN, EX \"LAJA\""), eventin.aliases(), "quoted comma and doubled quotes");

        MaritimeList.Listing runner = byId.get("shadow-runner");
        assertNull(runner.imo());
        assertEquals(List.of("613000001", "613000002"), runner.mmsi());

        assertEquals(List.of("WARNED\nSECOND LINE"), byId.get("warned").aliases(), "line break inside quotes");
    }

    @Test
    void refusesAFileWhoseColumnsChanged() {
        IOException e = assertThrows(IOException.class, () -> MaritimeList.parse(new StringReader(
                "\"type\",\"caption\",\"imo\"\n\"VESSEL\",\"X\",\"IMO9308065\"\n")));
        assertTrue(e.getMessage().contains("risk"), e.getMessage());
    }

    @Test
    void refusesAnUnterminatedQuote() {
        String header = String.join(",", MaritimeList.REQUIRED_COLUMNS) + "\n";
        assertThrows(IOException.class, () -> MaritimeList.parse(new StringReader(header + "\"VESSEL,\"X\n")));
    }
}
