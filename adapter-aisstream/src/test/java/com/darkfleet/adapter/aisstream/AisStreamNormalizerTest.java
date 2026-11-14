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
        assertNull(msg.vesselName(), "identity comes from static messages, not positions");
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

    @Test
    void shouldReadDimensionsAndEtaFromShipStaticData() {
        // Shape of a real aisstream ShipStaticData frame (14 November 2026)
        String raw = """
                {
                  "MessageType": "ShipStaticData",
                  "MetaData": {"MMSI": 219001122, "ShipName": "SVITZER", "time_utc": "2026-11-14 16:02:19.455130066 +0000 UTC"},
                  "Message": {
                    "ShipStaticData": {
                      "UserID": 219001122, "ImoNumber": 0, "Name": "SVITZER@@@@", "CallSign": "OXAA2",
                      "Type": 52, "MaximumStaticDraught": 5.7, "Destination": "SKAGEN",
                      "Dimension": {"A": 15, "B": 35, "C": 10, "D": 5},
                      "Eta": {"Month": 11, "Day": 11, "Hour": 8, "Minute": 0}
                    }
                  }
                }
                """;
        NormalizedAisMessage msg = normalizer.normalize(raw).orElseThrow();
        assertEquals(AisMessageType.STATIC_VOYAGE, msg.messageType());
        assertNull(msg.imoNumber(), "IMO 0 means none");
        assertEquals("SVITZER", msg.vesselName());
        assertEquals(52, msg.shipType());
        assertEquals(50, msg.lengthMeters());
        assertEquals(15, msg.beamMeters());
        assertEquals(java.time.Instant.parse("2026-11-11T08:00:00Z"), msg.eta());
    }

    @Test
    void shouldTreatNotAvailableEtaAndZeroTypeAsMissing() {
        String raw = """
                {
                  "MessageType": "ShipStaticData",
                  "MetaData": {"MMSI": 219001122, "time_utc": "2026-11-14 16:02:19 +0000 UTC"},
                  "Message": {"ShipStaticData": {"UserID": 219001122, "Type": 0, "MaximumStaticDraught": 0,
                    "Dimension": {"A": 0, "B": 0, "C": 0, "D": 0},
                    "Eta": {"Month": 0, "Day": 0, "Hour": 24, "Minute": 60}}}
                }
                """;
        NormalizedAisMessage msg = normalizer.normalize(raw).orElseThrow();
        assertNull(msg.shipType());
        assertNull(msg.draughtMeters());
        assertNull(msg.lengthMeters());
        assertNull(msg.eta());
    }

    @Test
    void dropsFramesWithoutTimeAndKeepsMissingSpeedEmpty() {
        String noTime = """
                {"MessageType": "PositionReport", "MetaData": {"MMSI": 273399110},
                 "Message": {"PositionReport": {"Latitude": 59.8, "Longitude": 25.4, "Sog": 11.8}}}
                """;
        assertTrue(normalizer.normalize(noTime).isEmpty(), "no time_utc: dropped, not stamped with 'now'");

        String noSpeed = """
                {"MessageType": "PositionReport", "MetaData": {"MMSI": 273399110, "time_utc": "2026-11-14 16:02:19 +0000 UTC"},
                 "Message": {"PositionReport": {"Latitude": 59.8, "Longitude": 25.4, "Cog": 360, "TrueHeading": 511}}}
                """;
        NormalizedAisMessage msg = normalizer.normalize(noSpeed).orElseThrow();
        assertNull(msg.sogKnots(), "missing speed must not look like a stopped ship");
        assertNull(msg.cogDegrees(), "360 = not available");
        assertNull(msg.headingDegrees(), "511 = not available");
    }
}
