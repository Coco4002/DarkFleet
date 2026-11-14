package com.darkfleet.common.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AisEtaTest {

    private static final Instant REPORTED = Instant.parse("2026-11-14T16:00:00Z");

    @Test
    void resolvesToTheCurrentYear() {
        assertEquals(Instant.parse("2026-11-20T08:30:00Z"), AisEta.resolve(11, 20, 8, 30, REPORTED));
    }

    @Test
    void keepsARecentlyPassedEtaInTheSameYear() {
        assertEquals(Instant.parse("2026-11-02T03:00:00Z"), AisEta.resolve(11, 2, 3, 0, REPORTED));
    }

    @Test
    void rollsOverToNextYear() {
        Instant lateDecember = Instant.parse("2026-12-28T12:00:00Z");
        assertEquals(Instant.parse("2027-01-03T06:00:00Z"), AisEta.resolve(1, 3, 6, 0, lateDecember));
    }

    @Test
    void notAvailableValuesGiveNull() {
        assertNull(AisEta.resolve(0, 0, 24, 60, REPORTED));
        assertNull(AisEta.resolve(2, 31, 12, 0, REPORTED), "no such date");
    }

    @Test
    void leapDayFindsTheNextLeapYear() {
        Instant late2027 = Instant.parse("2027-12-01T00:00:00Z");
        assertEquals(Instant.parse("2028-02-29T12:00:00Z"), AisEta.resolve(2, 29, 12, 0, late2027));
    }

    @Test
    void decodesDigitrafficPackedEta() {
        // Real values from the Digitraffic MQTT stream on 14 November 2026
        assertEquals(Instant.parse("2026-11-14T08:00:00Z"), AisEta.fromPacked(750080, REPORTED));
        assertEquals(Instant.parse("2026-11-02T03:00:00Z"), AisEta.fromPacked(725184, REPORTED));
        assertNull(AisEta.fromPacked(0, REPORTED));
    }

    @Test
    void dimensionsNeedBothSides() {
        assertEquals(250, NormalizedAisMessage.dimension(202, 48));
        assertNull(NormalizedAisMessage.dimension(0, 48));
        assertNull(NormalizedAisMessage.dimension(null, 48));
    }
}
