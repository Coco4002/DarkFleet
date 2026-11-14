package com.darkfleet.common.kafka;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

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
}
