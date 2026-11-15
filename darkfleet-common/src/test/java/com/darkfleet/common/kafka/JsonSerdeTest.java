package com.darkfleet.common.kafka;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.CoverageCell;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonSerdeTest {

    private final JsonSerde<NormalizedAisMessage> serde = new JsonSerde<>(NormalizedAisMessage.class);

    @Test
    void roundTripsAMessageWithIsoDates() {
        NormalizedAisMessage original = NormalizedAisMessage.positionReport(
                "273450990", Instant.parse("2026-11-14T15:53:22Z"), 59.795, 25.41, 12.5, 250.0, 250.0, 0,
                AisSource.AISSTREAM, false);

        byte[] bytes = serde.serializer().serialize("ais.raw", original);
        String json = new String(bytes, StandardCharsets.UTF_8);

        assertTrue(json.contains("\"eventTime\":\"2026-11-14T15:53:22Z\""), json);
        assertFalse(json.contains("cargoOrTanker"), "derived getters must not be serialized: " + json);
        assertEquals(original, serde.deserializer().deserialize("ais.raw", bytes));
    }

    @Test
    void readsMessagesWrittenBeforeTheJackson3Upgrade() {
        // A real line recorded by the coverage test (Jackson 2 era), plus a field this version doesn't know.
        String recorded = """
                {"mmsi":"230193580","eventTime":"2026-11-14T15:53:22Z","latitude":62.11882,"longitude":28.31171,\
                "sogKnots":0.0,"cogDegrees":360.0,"headingDegrees":null,"navStatus":0,"source":"DIGITRAFFIC",\
                "messageType":"POSITION_REPORT","ingestionTime":"2026-11-14T15:53:24.334618603Z","historical":false,\
                "imoNumber":null,"vesselName":null,"callSign":null,"shipType":null,"draughtMeters":null,\
                "destination":null,"addedByANewerProducer":42}""";

        NormalizedAisMessage msg = serde.deserializer().deserialize("ais.raw", recorded.getBytes(StandardCharsets.UTF_8));

        assertEquals("230193580", msg.mmsi());
        assertEquals(Instant.parse("2026-11-14T15:53:22Z"), msg.eventTime());
        assertEquals(AisSource.DIGITRAFFIC, msg.source());
        assertEquals(Instant.parse("2026-11-14T15:53:24.334618603Z"), msg.ingestionTime());
    }

    @Test
    void roundTripsEveryTopicRecord() {
        Instant t = Instant.parse("2026-11-15T10:00:00Z");
        roundTrip(new TrackPoint("273450990", t, 59.8, 25.4, 11.5, 254.0, null, 0, AisSource.DIGITRAFFIC, "861126d37ffffff"));
        roundTrip(new VesselStatic("273450990", t, AisSource.AISSTREAM, "9312456", "NEVA SPIRIT", "UBCD4", 81, 13.8,
                "PRIMORSK", 244, 42, t.plusSeconds(86_400)));
        roundTrip(new CoverageCell("861126d37ffffff", t, Map.of(AisSource.AISSTREAM, 12L), 3, 1,
                Map.of(AisSource.AISSTREAM, 300L, AisSource.DIGITRAFFIC, 900L), 2));
        roundTrip(new DetectionRules(3, 60, 5, 5, 3.0, 120, 500, 2.0, 120, 24, 0.4));
        Detection open = new Detection("gap:273450990:1763200800", DetectionType.UNEXPLAINED_GAP, DetectionStatus.OPEN,
                1, "273450990", null, t, null, 59.8, 25.4, "861126d37ffffff", 0,
                Map.of("watchingFeeds", "AISSTREAM", "lastSpeedKnots", 11.5, "gapMinutes", 60));
        roundTrip(open);
        roundTrip(open.closed(t.plusSeconds(7200), Map.of("closeReason", "reported again")));
    }

    @SuppressWarnings("unchecked")
    private static <T> void roundTrip(T value) {
        var serde = new JsonSerde<>((Class<T>) value.getClass());
        assertEquals(value, serde.deserializer().deserialize("t", serde.serializer().serialize("t", value)));
    }
}
