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

class IdentityTest {

    private static final Instant START = Instant.parse("2026-11-10T10:00:00Z");

    private static void staticAt(Harness h, Duration after, String mmsi, String imo) {
        h.now = START.plus(after);
        h.tanker(mmsi, AISSTREAM, imo, 10.0);
    }

    @Test
    void oneMmsiAlternatingBetweenTwoImoNumbersIsAConflictAndClosesWhenOneStops() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            staticAt(h, Duration.ZERO, "273500001", "9312456");
            staticAt(h, Duration.ofMinutes(6), "273500001", "9074729");
            assertTrue(h.detections().isEmpty(), "a single switch is a correction, not a conflict");
            staticAt(h, Duration.ofMinutes(12), "273500001", "9312456");

            List<Detection> opened = h.detections();
            assertEquals(1, opened.size());
            assertEquals(DetectionType.IDENTITY_INCONSISTENCY, opened.getFirst().type());
            assertEquals("9074729, 9312456", opened.getFirst().evidence().get("imoNumbers"));

            // the other feed's first report arrives late; it must not move the conflict's last sighting back
            h.now = START.plus(Duration.ofMinutes(7));
            h.staticData("273500001", com.darkfleet.common.model.AisSource.DIGITRAFFIC, "9074729", "TANKER 273500001", 80, 10.0);

            h.now = START.plus(Duration.ofMinutes(12));
            h.advance(Duration.ofHours(25));
            Detection closed = h.detections().getFirst();
            assertEquals(DetectionStatus.CLOSED, closed.status());
            assertEquals(START.plus(Duration.ofMinutes(12)), closed.endedAt());
        }
    }

    @Test
    void oneImoBroadcastByTwoMmsisAtTheSameTimeIsAConflictButARefleggingIsNot() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            // re-flagged: the old MMSI stops before the new one starts
            staticAt(h, Duration.ZERO, "273500002", "9074729");
            staticAt(h, Duration.ofHours(2), "273500003", "9074729");
            staticAt(h, Duration.ofHours(4), "273500003", "9074729");
            assertTrue(h.detections().isEmpty());

            // cloned: two MMSIs keep using the same IMO
            staticAt(h, Duration.ofHours(5), "273500004", "9312456");
            staticAt(h, Duration.ofHours(6), "636000001", "9312456");
            staticAt(h, Duration.ofHours(7), "273500004", "9312456");
            List<Detection> opened = h.detections();
            assertEquals(1, opened.size());
            Detection conflict = opened.getFirst();
            assertEquals("273500004", conflict.mmsi());
            assertEquals("636000001", conflict.otherMmsi());
            assertEquals("9312456", conflict.evidence().get("imoNumber"));
        }
    }

    @Test
    void anImoConflictClosesWhenOneMmsiHasStoppedEvenIfTheOtherKeepsReporting() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            staticAt(h, Duration.ZERO, "273500004", "9312456");
            staticAt(h, Duration.ofHours(1), "636000001", "9312456");
            staticAt(h, Duration.ofHours(2), "273500004", "9312456");
            assertEquals(1, h.detections().size());
            // only the first MMSI goes on; its report a day later is what retires the other
            staticAt(h, Duration.ofHours(27), "273500004", "9312456");
            Detection closed = h.detections().getFirst();
            assertEquals(DetectionStatus.CLOSED, closed.status());
            assertEquals(START.plus(Duration.ofHours(2)), closed.endedAt());
        }
    }

    @Test
    void mistypedImoNumbersAreIgnored() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            staticAt(h, Duration.ZERO, "273500005", "9312456");
            staticAt(h, Duration.ofMinutes(6), "273500005", "9312457"); // bad check digit
            staticAt(h, Duration.ofMinutes(12), "273500005", "9312456");
            assertTrue(h.detections().isEmpty());
        }
    }
}
