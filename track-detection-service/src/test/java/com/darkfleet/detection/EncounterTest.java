package com.darkfleet.detection;

import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.detection.support.Geo;
import com.darkfleet.detection.support.Grid;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.darkfleet.common.model.AisSource.AISSTREAM;
import static com.darkfleet.common.model.AisSource.DIGITRAFFIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncounterTest {

    private static final Instant START = Instant.parse("2026-11-10T10:00:00Z");
    private static final String A = "273400001";
    private static final String B = "273400002";

    /** Two points 300 m apart on either side of a cell border. */
    private static double[][] straddlingBorder() {
        for (double lon = 26.0; lon < 26.2; lon += 0.0005) {
            double[] east = Geo.destination(59.9, lon, 90, 300);
            if (!Grid.cell(59.9, lon).equals(Grid.cell(east[0], east[1]))) {
                return new double[][]{{59.9, lon}, east};
            }
        }
        throw new IllegalStateException("no cell border found");
    }

    @Test
    void twoSlowTankersSideBySideAcrossACellBorderMakeOneEncounter() {
        double[][] spots = straddlingBorder();
        assertNotEquals(Grid.cell(spots[0][0], spots[0][1]), Grid.cell(spots[1][0], spots[1][1]));
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.tanker(A, AISSTREAM, "9312456", 14.5);
            h.tanker(B, DIGITRAFFIC, "9074729", 6.0);
            for (int i = 0; i <= 125; i += 2) {
                Instant at = START.plus(Duration.ofMinutes(i));
                h.position(A, AISSTREAM, at, spots[0][0], spots[0][1], 0.5, 90.0, 1);
                h.position(B, DIGITRAFFIC, at.plusSeconds(30), spots[1][0], spots[1][1], 0.3, 270.0, 1);
                if (i == 100) { // the laden one discharges, the other loads
                    h.now = at;
                    h.tanker(A, AISSTREAM, "9312456", 9.5);
                    h.tanker(B, DIGITRAFFIC, "9074729", 11.0);
                }
            }
            List<Detection> opened = h.detections().stream().filter(d -> d.type() == DetectionType.POTENTIAL_ENCOUNTER).toList();
            assertEquals(1, opened.size(), "seen in both cells, raised once");
            Detection encounter = opened.getFirst();
            assertEquals(A, encounter.mmsi());
            assertEquals(B, encounter.otherMmsi());
            assertEquals(START.plusSeconds(30), encounter.startedAt());

            h.now = START.plus(Duration.ofMinutes(126));
            h.advance(Duration.ofMinutes(32));
            Detection closed = h.detections().stream().filter(d -> d.type() == DetectionType.POTENTIAL_ENCOUNTER).findFirst().orElseThrow();
            assertEquals(DetectionStatus.CLOSED, closed.status());
            assertEquals(-5.0, (double) closed.evidence().get("draughtChangeAMeters"), 0.01);
            assertEquals(5.0, (double) closed.evidence().get("draughtChangeBMeters"), 0.01);
            assertEquals(true, closed.evidence().get("draughtChanged"));
        }
    }

    @Test
    void inReplayAnEncounterClosesWhileOnlyOtherVesselsKeepReporting() {
        try (Harness h = new Harness(DetectionMode.REPLAY, START)) {
            h.tanker(A, com.darkfleet.common.model.AisSource.DMA_REPLAY, "9312456", 14.5);
            h.tanker(B, com.darkfleet.common.model.AisSource.DMA_REPLAY, "9074729", 6.0);
            for (int i = 0; i <= 125; i += 2) {
                Instant at = START.plus(Duration.ofMinutes(i));
                h.position(A, com.darkfleet.common.model.AisSource.DMA_REPLAY, at, 59.9, 26.0, 0.5, 90.0, 1);
                h.position(B, com.darkfleet.common.model.AisSource.DMA_REPLAY, at, 59.9, 26.002, 0.3, 90.0, 1);
                h.tick();
            }
            assertEquals(1, encounters(h).size());
            for (int i = 126; i <= 170; i++) { // the pair is silent; someone else keeps the replay going
                h.position("219000001", com.darkfleet.common.model.AisSource.DMA_REPLAY, START.plus(Duration.ofMinutes(i)),
                        55.0, 11.0, 10.0, 90.0, 0);
                h.tick();
            }
            List<Detection> closed = encounters(h);
            assertEquals(1, closed.size());
            assertEquals(DetectionStatus.CLOSED, closed.getFirst().status());
        }
    }

    private static List<Detection> encounters(Harness h) {
        return h.detections().stream().filter(d -> d.type() == DetectionType.POTENTIAL_ENCOUNTER).toList();
    }

    @Test
    void passingVesselsAndShortMeetingsAreNotEncounters() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.tanker(A, AISSTREAM, "9312456", 14.5);
            h.tanker(B, AISSTREAM, "9074729", 6.0);
            for (int i = 0; i <= 125; i += 2) {
                Instant at = START.plus(Duration.ofMinutes(i));
                h.position(A, AISSTREAM, at, 59.9, 26.0, 1.0, 90.0, 0);
                // B passes at speed, then sits 2 km away
                h.position(B, AISSTREAM, at, 59.9, 26.001, i < 10 ? 12.0 : 0.5, 90.0, 0);
                if (i >= 10) {
                    h.position(B, AISSTREAM, at.plusSeconds(1), 59.918, 26.0, 0.5, 90.0, 0);
                }
            }
            assertTrue(h.detections().stream().noneMatch(d -> d.type() == DetectionType.POTENTIAL_ENCOUNTER));
        }
    }
}
