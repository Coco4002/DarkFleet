package com.darkfleet.adapter.aisstream;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class AisStreamNormalizerTest {

    private final AisStreamNormalizer normalizer = new AisStreamNormalizer();

    @Test
    void shouldNormalizePositionReportFrame() {
        String raw = """
                {
                  "MessageType": "PositionReport",
                  "MetaData": {
                    "MMSI": 273399110,
                    "ShipName": "NEVA SPIRIT   ",
                    "latitude": 59.825,
                    "longitude": 25.412,
                    "time_utc": "2026-11-10 08:15:30 +0000 UTC"
                  },
                  "Message": {
                    "PositionReport": {
                      "UserID": 273399110,
                      "Latitude": 59.825,
                      "Longitude": 25.412,
                      "Sog": 11.8,
                      "Cog": 254.2,
                      "TrueHeading": 253,
                      "NavigationalStatus": 0
                    }
                  }
                }
                """;

        Optional<NormalizedAisMessage> result = normalizer.normalize(raw);
        assertTrue(result.isPresent());
        NormalizedAisMessage msg = result.get();
        assertEquals("273399110", msg.mmsi());
        assertEquals(AisSource.AISSTREAM, msg.source());
        assertEquals(AisMessageType.POSITION_REPORT, msg.messageType());
        assertEquals("NEVA SPIRIT", msg.vesselName());
        assertEquals(59.825, msg.latitude(), 0.0001);
        assertEquals(25.412, msg.longitude(), 0.0001);
        assertEquals(11.8, msg.sogKnots(), 0.01);
        assertFalse(msg.historical());
    }

    @Test
    void shouldNormalizeShipStaticDataFrame() {
        String raw = """
                {
                  "MessageType": "ShipStaticData",
                  "MetaData": {
                    "MMSI": 273399110,
                    "ShipName": "NEVA SPIRIT",
                    "time_utc": "2026-11-10T08:16:00Z"
                  },
                  "Message": {
                    "ShipStaticData": {
                      "UserID": 273399110,
                      "ImoNumber": 9312456,
                      "Name": "NEVA SPIRIT@@@@",
                      "CallSign": "UBCD4",
                      "Type": 81,
                      "MaximumStaticDraught": 13.8,
                      "Destination": "SKAGEN"
                    }
                  }
                }
                """;

        Optional<NormalizedAisMessage> result = normalizer.normalize(raw);
        assertTrue(result.isPresent());
        NormalizedAisMessage msg = result.get();
        assertEquals("273399110", msg.mmsi());
        assertEquals(AisMessageType.STATIC_VOYAGE, msg.messageType());
        assertEquals("9312456", msg.imoNumber());
        assertEquals("NEVA SPIRIT", msg.vesselName());
        assertEquals(81, msg.shipType());
        assertEquals(13.8, msg.draughtMeters(), 0.01);
        assertTrue(msg.isCargoOrTanker());
    }
}
