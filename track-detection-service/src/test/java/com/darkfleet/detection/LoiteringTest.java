package com.darkfleet.detection;

import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.darkfleet.common.model.AisSource.AISSTREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoiteringTest {

    private static final Instant START = Instant.parse("2026-11-10T10:00:00Z");
    private static final String MMSI = "273300001";

    @Test
    void opensAfterTwoSlowHoursAndClosesWhenTheVesselMovesOn() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.tanker(MMSI, AISSTREAM, "9312456", 12.0);
            for (int i = 0; i <= 125; i += 5) {
                h.position(MMSI, AISSTREAM, START.plus(Duration.ofMinutes(i)), 59.9, 26.0 + i * 0.0002, 1.5, 90.0, 0);
            }
            List<Detection> opened = h.detections();
            assertEquals(1, opened.size());
            assertEquals(DetectionType.LOITERING, opened.getFirst().type());
            assertEquals(START, opened.getFirst().startedAt());

            Instant moving = START.plus(Duration.ofMinutes(130));
            h.position(MMSI, AISSTREAM, moving, 59.9, 26.03, 9.0, 90.0, 0);
            assertTrue(h.detections().isEmpty(), "one fast report is not enough");
            h.position(MMSI, AISSTREAM, moving.plusSeconds(60), 59.9, 26.035, 9.5, 90.0, 0);
            Detection closed = h.detections().getFirst();
            assertEquals(DetectionStatus.CLOSED, closed.status());
            assertEquals(START.plus(Duration.ofMinutes(125)), closed.endedAt());
            assertEquals("moving again", closed.evidence().get("closeReason"));
        }
    }

    @Test
    void endsWhenSlowReportsStopAndIgnoresPortsAndCargoTypesItDoesNotWatch() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.tanker(MMSI, AISSTREAM, "9312456", 12.0);
            h.staticData("273300002", AISSTREAM, null, "TUG", 52, 4.0);
            for (int i = 0; i <= 130; i += 5) {
                Instant at = START.plus(Duration.ofMinutes(i));
                h.position(MMSI, AISSTREAM, at, 59.9, 26.0, 1.0, 90.0, 0);
                h.position("273300002", AISSTREAM, at, 59.95, 26.0, 1.0, 90.0, 0);
            }
            assertEquals(1, h.detections().size());
            h.now = START.plus(Duration.ofMinutes(130));
            h.advance(Duration.ofMinutes(32));
            Detection closed = h.detections().getFirst();
            assertEquals("no slow reports", closed.evidence().get("closeReason"));
        }
    }
}
