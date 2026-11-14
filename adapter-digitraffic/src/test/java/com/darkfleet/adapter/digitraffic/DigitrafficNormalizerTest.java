package com.darkfleet.adapter.digitraffic;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DigitrafficNormalizerTest {

    private final DigitrafficNormalizer normalizer = new DigitrafficNormalizer();

    @Test
    void shouldNormalizeDigitrafficLocationTopic() {
        String topic = "vessels-v2/230987000/location";
        String payload = """
                {
                  "time": 1762762200,
                  "sog": 12.4,
                  "cog": 82.5,
                  "navStat": 0,
                  "rot": 0,
                  "posAcc": true,
                  "raim": false,
                  "heading": 83,
                  "lon": 24.9812,
                  "lat": 59.9104
                }
                """;

        Optional<NormalizedAisMessage> result = normalizer.normalizeMqttPayload(topic, payload);
        assertTrue(result.isPresent());
        NormalizedAisMessage msg = result.get();
        assertEquals("230987000", msg.mmsi());
        assertEquals(AisSource.DIGITRAFFIC, msg.source());
        assertEquals(AisMessageType.POSITION_REPORT, msg.messageType());
        assertEquals(59.9104, msg.latitude(), 0.0001);
        assertEquals(24.9812, msg.longitude(), 0.0001);
        assertEquals(12.4, msg.sogKnots(), 0.01);
        assertFalse(msg.historical());
    }

    @Test
    void shouldNormalizeDigitrafficMetadataTopicScalingDraught() {
        String topic = "vessels-v2/273399110/metadata";
        String payload = """
                {
                  "timestamp": 1762762200000,
                  "destination": "PRIMORSK",
                  "name": "BALTIC PIONEER",
                  "draught": 142,
                  "eta": 173240,
                  "posType": 1,
                  "refA": 180,
                  "refB": 35,
                  "refC": 16,
                  "refD": 16,
                  "callSign": "UBBP9",
                  "imo": 9288104,
                  "type": 80
                }
                """;

        Optional<NormalizedAisMessage> result = normalizer.normalizeMqttPayload(topic, payload);
        assertTrue(result.isPresent());
        NormalizedAisMessage msg = result.get();
        assertEquals("273399110", msg.mmsi());
        assertEquals(AisMessageType.STATIC_VOYAGE, msg.messageType());
        assertEquals("9288104", msg.imoNumber());
        assertEquals("BALTIC PIONEER", msg.vesselName());
        assertEquals(80, msg.shipType());
        assertEquals(14.2, msg.draughtMeters(), 0.01);
        assertTrue(msg.isCargoOrTanker());
    }

    @Test
    void shouldTreatZeroTypeAndDraughtAsMissingAndScaleSmallDraught() {
        String withZeros = """
                {"timestamp": 1762762200000, "name": "EVA 316", "draught": 0, "imo": 7917977, "type": 0}
                """;
        NormalizedAisMessage zeros = normalizer.normalizeMqttPayload("vessels-v2/276000001/metadata", withZeros).orElseThrow();
        assertNull(zeros.shipType());
        assertNull(zeros.draughtMeters());

        String smallShip = """
                {"timestamp": 1762762200000, "name": "RIX TERRA", "draught": 30, "imo": 9100188, "shipType": 70}
                """;
        NormalizedAisMessage small = normalizer.normalizeMqttPayload("vessels-v2/276000002/metadata", smallShip).orElseThrow();
        assertEquals(70, small.shipType());
        assertEquals(3.0, small.draughtMeters(), 0.01);
    }
}
