package com.darkfleet.detection;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.darkfleet.common.model.AisSource.AISSTREAM;
import static com.darkfleet.common.model.AisSource.DIGITRAFFIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GapTest {

    private static final Instant START = Instant.parse("2026-11-10T10:00:00Z");
    private static final String MMSI = "273200001";
    private static final double LAT = 59.90;
    private static final double LON = 26.00;

    /** A tanker reporting for five minutes from a well-covered spot, then going silent. */
    private static Harness silentTanker(AisSource... feeds) {
        Harness h = new Harness(DetectionMode.LIVE, START);
        h.cover(LAT, LON);
        for (AisSource feed : feeds) {
            h.feedWorking(feed);
            h.tanker(MMSI, feed, "9312456", 12.0);
        }
        for (int i = 0; i <= 5; i++) {
            for (AisSource feed : feeds) {
                h.position(MMSI, feed, START.plus(Duration.ofMinutes(i)), LAT, LON, 0.4, null, 0);
            }
        }
        return h;
    }

    @Test
    void opensWhenAWorkingFeedCoveringTheSpotStopsHearingTheVesselAndClosesWhenItReturns() {
        try (Harness h = silentTanker(AISSTREAM, DIGITRAFFIC)) {
            h.advance(Duration.ofMinutes(64), AISSTREAM, DIGITRAFFIC);
            assertTrue(h.detections().isEmpty(), "not yet silent for 60 minutes");

            h.advance(Duration.ofMinutes(2), AISSTREAM, DIGITRAFFIC);
            List<Detection> opened = h.detections();
            assertEquals(1, opened.size());
            Detection gap = opened.getFirst();
            assertEquals(DetectionType.UNEXPLAINED_GAP, gap.type());
            assertEquals(DetectionStatus.OPEN, gap.status());
            assertEquals(START.plus(Duration.ofMinutes(5)), gap.startedAt());
            assertEquals("AISSTREAM, DIGITRAFFIC", gap.evidence().get("watchingFeeds"));

            h.advance(Duration.ofMinutes(30), AISSTREAM, DIGITRAFFIC);
            assertTrue(h.detections().isEmpty(), "raised once");

            h.position(MMSI, AISSTREAM, h.now, LAT + 0.1, LON, 11.0, 90.0, 0);
            Detection closed = h.detections().getFirst();
            assertEquals(DetectionStatus.CLOSED, closed.status());
            assertEquals(2, closed.version());
            assertEquals(gap.detectionId(), closed.detectionId());
            assertEquals("reported again", closed.evidence().get("closeReason"));
            assertEquals(11.1, (double) closed.evidence().get("distanceKm"), 0.1);
        }
    }

    @Test
    void aDeadFeedRaisesNothingAndOnceBackItGetsAFullGapTimeBeforeAnything() {
        try (Harness h = silentTanker(AISSTREAM)) {
            h.advance(Duration.ofMinutes(90)); // no heartbeats: feed down
            assertTrue(h.detections().isEmpty());

            h.advance(Duration.ofMinutes(59), AISSTREAM); // back, but not yet for the gap time
            assertTrue(h.detections().isEmpty());

            h.advance(Duration.ofMinutes(2), AISSTREAM);
            assertEquals(1, h.detections().size());
        }
    }

    @Test
    void oneDeadFeedDoesNotHideAGapSeenByTheOther() {
        try (Harness h = silentTanker(AISSTREAM, DIGITRAFFIC)) {
            h.advance(Duration.ofMinutes(66), DIGITRAFFIC);
            List<Detection> opened = h.detections();
            assertEquals(1, opened.size());
            assertEquals("DIGITRAFFIC", opened.getFirst().evidence().get("watchingFeeds"));
        }
    }

    @Test
    void nothingWhereCoverageIsUnknownOrWhereTheVesselWouldHaveSailedOutOfIt() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.feedWorking(AISSTREAM);
            h.tanker("273200002", AISSTREAM, "9312456", 12.0);
            h.position("273200002", AISSTREAM, START, 59.5, 22.0, 10.0, 90.0, 0); // no coverage known there

            h.cover(LAT, LON);
            h.tanker("273200003", AISSTREAM, "9074729", 12.0);
            h.position("273200003", AISSTREAM, START, LAT, LON, 12.0, 270.0, 0); // heading out of the covered cell

            h.advance(Duration.ofMinutes(120), AISSTREAM);
            assertTrue(h.detections().isEmpty());
        }
    }

    @Test
    void nothingForOtherShipTypesMooredVesselsOrPorts() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.feedWorking(AISSTREAM);
            h.cover(LAT, LON);
            h.staticData("273200004", AISSTREAM, null, "FERRY", 60, 5.0);
            h.position("273200004", AISSTREAM, START, LAT, LON, 0.0, null, 0);
            h.tanker("273200005", AISSTREAM, "9312456", 12.0);
            h.position("273200005", AISSTREAM, START, LAT, LON, 0.0, null, 5); // moored
            h.advance(Duration.ofMinutes(120), AISSTREAM);
            assertTrue(h.detections().isEmpty());
        }
    }

    @Test
    void nothingForAMovingVesselWhoseCourseIsUnknown() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.feedWorking(AISSTREAM);
            h.cover(LAT, LON);
            h.tanker(MMSI, AISSTREAM, "9312456", 12.0);
            h.position(MMSI, AISSTREAM, START, LAT, LON, 12.0, null, 0);
            h.advance(Duration.ofMinutes(120), AISSTREAM);
            assertTrue(h.detections().isEmpty(), "where it went is unknown, so whether it left coverage is too");
        }
    }

    @Test
    void outdatedCoverageDoesNotCount() {
        try (Harness h = new Harness(DetectionMode.LIVE, START)) {
            h.feedWorking(AISSTREAM);
            h.coverAt(LAT, LON, START.minus(Duration.ofHours(23))); // the cell's last record, long ago
            h.tanker(MMSI, AISSTREAM, "9312456", 12.0);
            h.position(MMSI, AISSTREAM, START, LAT, LON, 0.2, null, 0);
            h.advance(Duration.ofMinutes(120), AISSTREAM);
            assertTrue(h.detections().isEmpty());
        }
    }

    @Test
    void aDelayedStaticReportFromTheOtherFeedDoesNotOverwriteNewerData() {
        try (Harness h = silentTanker(AISSTREAM)) {
            h.now = START.minus(Duration.ofHours(1));
            h.tanker(MMSI, DIGITRAFFIC, "9312456", 8.0); // stamped an hour earlier, arrives late
            h.now = START;
            h.advance(Duration.ofMinutes(66), AISSTREAM);
            Detection gap = h.detections().getFirst();
            assertEquals(12.0, (double) gap.evidence().get("draughtMeters"));
        }
    }

    @Test
    void approvedRulesReplaceTheBaseline() {
        try (Harness h = silentTanker(AISSTREAM)) {
            h.approve(new DetectionRules(1, 30, 5, 5, 3.0, 120, 500, 2.0, 120, 24, 0.4));
            h.advance(Duration.ofMinutes(36), AISSTREAM);
            List<Detection> opened = h.detections();
            assertEquals(1, opened.size());
            assertEquals(1, opened.getFirst().rulesVersion());
        }
    }

    @Test
    void replayKeepsTimeByTheMessagesAndNeedsNoHeartbeats() {
        try (Harness h = new Harness(DetectionMode.REPLAY, START)) {
            h.cover(LAT, LON);
            h.tanker(MMSI, AisSource.DMA_REPLAY, "9312456", 12.0);
            h.position(MMSI, AisSource.DMA_REPLAY, START, LAT, LON, 0.4, null, 0);
            // another vessel's reports move the replay's clock on
            for (int i = 1; i <= 70; i++) {
                h.position("219000001", AisSource.DMA_REPLAY, START.plus(Duration.ofMinutes(i)), 55.0, 11.0, 10.0, 90.0, 0);
            }
            List<Detection> detections = h.detections();
            assertEquals(1, detections.size());
            assertEquals(DetectionType.UNEXPLAINED_GAP, detections.getFirst().type());
        }
    }
}
