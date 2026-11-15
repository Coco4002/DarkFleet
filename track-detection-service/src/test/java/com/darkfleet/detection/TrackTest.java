package com.darkfleet.detection;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrackTest {

    private static final Instant START = Instant.parse("2026-11-10T10:00:00Z");

    @Test
    void mergesBothFeedsIntoOneThinnedTrack() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            // ten minutes of a report every 10 s, each received through both feeds
            for (int i = 0; i < 60; i++) {
                Instant at = START.plusSeconds(10L * i);
                double lon = 26.0 + i * 0.0005;
                h.position("273100001", AisSource.DIGITRAFFIC, at, 59.9, lon, 10.0, 90.0, 0);
                h.position("273100001", AisSource.AISSTREAM, at, 59.9, lon, 10.0, 90.0, 0);
            }
            List<TrackPoint> points = h.trackPoints();
            assertEquals(10, points.size(), "one point a minute, duplicates through the second feed dropped");
            assertTrue(points.stream().allMatch(p -> p.h3Cell().length() == 15));
        }
    }

    @Test
    void turnsAndSpeedChangesAddPointsAndOlderPositionsAreIgnored() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.position("273100002", AisSource.AISSTREAM, START, 59.9, 26.0, 10.0, 90.0, 0);
            h.position("273100002", AisSource.AISSTREAM, START.plusSeconds(10), 59.9, 26.001, 10.0, 130.0, 0); // turn
            h.position("273100002", AisSource.AISSTREAM, START.plusSeconds(20), 59.9, 26.002, 4.0, 130.0, 0);  // slowed
            h.position("273100002", AisSource.AISSTREAM, START.plusSeconds(30), 59.9, 26.003, 4.5, 131.0, 0);  // neither
            h.position("273100002", AisSource.DIGITRAFFIC, START.plusSeconds(5), 59.9, 26.0005, 10.0, 90.0, 0); // older
            assertEquals(3, h.trackPoints().size());
        }
    }

    @Test
    void staticDataIsPublishedWhenItChangesAndAgainAfterADay() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.staticData("273100003", AisSource.AISSTREAM, "9312456", "NEVA SPIRIT", 80, 12.0);
            h.now = START.plus(Duration.ofMinutes(6));
            h.staticData("273100003", AisSource.AISSTREAM, "9312456", "NEVA SPIRIT", 80, 12.0); // repeat
            h.now = START.plus(Duration.ofMinutes(12));
            h.staticData("273100003", AisSource.AISSTREAM, "9312456", "NEVA SPIRIT", 80, 8.5); // draught changed
            h.staticData("273100003", AisSource.DIGITRAFFIC, "9312456", "NEVA SPIRIT", 80, 8.5); // other feed
            h.now = START.plus(Duration.ofHours(25));
            h.staticData("273100003", AisSource.AISSTREAM, "9312456", "NEVA SPIRIT", 80, 8.5); // daily repeat
            List<VesselStatic> published = h.statics();
            assertEquals(4, published.size());
            assertEquals(8.5, published.get(1).draughtMeters());
        }
    }

    @Test
    void dropsMessagesStampedInTheFuture() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            Instant future = START.plus(Duration.ofHours(2));
            h.raw(new NormalizedAisMessage("273100004", future, 59.9, 26.0, 10.0, 90.0, null, 0, AisSource.AISSTREAM,
                    AisMessageType.POSITION_REPORT, START, false, null, null, null, null, null, null, null, null, null));
            assertTrue(h.trackPoints().isEmpty());
        }
    }
}
