package com.darkfleet.detection;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.DetectionEvent;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.VesselTrack;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TrackAndDetectionEngineTest {

    private CoverageTracker coverageTracker;
    private TrackAndDetectionEngine engine;

    @BeforeEach
    void setUp() {
        coverageTracker = new CoverageTracker();
        engine = new TrackAndDetectionEngine(coverageTracker);
    }

    @Test
    void shouldSuppressGapWhenSeenBySecondaryFeedAndFlagTrueUnexplainedGap() {
        Instant t0 = Instant.parse("2026-11-10T08:00:00Z").truncatedTo(ChronoUnit.HOURS);

        // Establish high coverage in Gulf of Finland cell (59.80, 25.40) across both feeds
        for (int i = 0; i < 5; i++) {
            String bgMmsi = "23000010" + i;
            for (int m = 0; m <= 60; m += 15) {
                Instant ts = t0.plus(Duration.ofMinutes(m));
                engine.processMessage(NormalizedAisMessage.positionReport(
                        bgMmsi, ts, 59.80, 25.40, 12.0, 260.0, 260.0, 0, AisSource.DIGITRAFFIC, true
                ));
                engine.processMessage(NormalizedAisMessage.positionReport(
                        bgMmsi, ts, 59.80, 25.40, 12.0, 260.0, 260.0, 0, AisSource.AISSTREAM, true
                ));
            }
        }

        // Vessel 1 (273310010): Lost on AISSTREAM for 70 min, but seen on DIGITRAFFIC at +30 min
        String crossCheckedMmsi = "273310010";
        engine.processMessage(NormalizedAisMessage.staticReport(
                crossCheckedMmsi, t0, AisSource.DIGITRAFFIC, true,
                "9299101", "LADOGA STAR", "UBLS1", 80, 13.0, "SKAGEN"
        ));
        engine.processMessage(NormalizedAisMessage.positionReport(
                crossCheckedMmsi, t0.plus(Duration.ofMinutes(5)),
                59.801, 25.401, 11.0, 255.0, 255.0, 0, AisSource.AISSTREAM, true
        ));
        engine.processMessage(NormalizedAisMessage.positionReport(
                crossCheckedMmsi, t0.plus(Duration.ofMinutes(35)),
                59.801, 25.360, 11.0, 255.0, 255.0, 0, AisSource.DIGITRAFFIC, true
        ));
        engine.processMessage(NormalizedAisMessage.positionReport(
                crossCheckedMmsi, t0.plus(Duration.ofMinutes(75)),
                59.801, 25.310, 11.0, 255.0, 255.0, 0, AisSource.AISSTREAM, true
        ));

        // Vessel 2 (273450990): True 80-minute gap on all feeds in the same well-covered cell
        String darkMmsi = "273450990";
        engine.processMessage(NormalizedAisMessage.staticReport(
                darkMmsi, t0, AisSource.AISSTREAM, true,
                "9341029", "SHADOW AURORA", "UBSA9", 80, 14.2, "FOR ORDERS"
        ));
        engine.processMessage(NormalizedAisMessage.positionReport(
                darkMmsi, t0.plus(Duration.ofMinutes(5)),
                59.802, 25.402, 12.0, 255.0, 255.0, 0, AisSource.AISSTREAM, true
        ));
        engine.processMessage(NormalizedAisMessage.positionReport(
                darkMmsi, t0.plus(Duration.ofMinutes(85)),
                59.780, 24.980, 12.0, 255.0, 255.0, 0, AisSource.AISSTREAM, true
        ));

        List<DetectionEvent> activeGaps = engine.listDetections(
                DetectionType.UNEXPLAINED_GAP, null, null, false
        );
        assertEquals(1, activeGaps.size());
        assertEquals(darkMmsi, activeGaps.get(0).mmsi());
        assertTrue(activeGaps.get(0).neutralTitle().contains("Unexplained AIS Gap"));

        List<DetectionEvent> suppressed = engine.listDetections(
                DetectionType.COVERAGE_GAP_SUPPRESSED, crossCheckedMmsi, null, true
        );
        assertEquals(1, suppressed.size());
        assertTrue(suppressed.get(0).seenBySecondaryFeed());
    }

    @Test
    void shouldDetectShipToShipEncounterAndDraughtChangeWhileIgnoringTugs() {
        Instant t0 = Instant.parse("2026-11-10T10:00:00Z");
        String tankerA = "273511200";
        String tankerB = "352899100";
        String tug = "219001122";

        engine.processMessage(NormalizedAisMessage.staticReport(
                tankerA, t0, AisSource.DMA_REPLAY, true,
                "9388112", "NEVA TITAN", "UBNT2", 80, 14.6, "PORT SAID"
        ));
        engine.processMessage(NormalizedAisMessage.staticReport(
                tankerB, t0, AisSource.DMA_REPLAY, true,
                "9400215", "AEGEAN HORIZON", "3FQM8", 80, 9.2, "ROTTERDAM"
        ));
        engine.processMessage(NormalizedAisMessage.staticReport(
                tug, t0, AisSource.DMA_REPLAY, true,
                "8911223", "SVITZER TUG", "OXAA2", 52, 4.8, "SKAGEN"
        ));

        for (int m = 0; m <= 45; m += 15) {
            Instant ts = t0.plus(Duration.ofMinutes(m));
            engine.processMessage(NormalizedAisMessage.positionReport(
                    tankerA, ts, 56.9500, 11.4500, 1.0, 180.0, 180.0, 0, AisSource.DMA_REPLAY, true
            ));
            engine.processMessage(NormalizedAisMessage.positionReport(
                    tankerB, ts, 56.9510, 11.4508, 1.0, 180.0, 180.0, 0, AisSource.DMA_REPLAY, true
            ));
            engine.processMessage(NormalizedAisMessage.positionReport(
                    tug, ts, 56.9505, 11.4504, 1.0, 180.0, 180.0, 0, AisSource.DMA_REPLAY, true
            ));
        }

        // Report draught change on Tanker A after the encounter
        engine.processMessage(NormalizedAisMessage.staticReport(
                tankerA, t0.plus(Duration.ofMinutes(55)), AisSource.DMA_REPLAY, true,
                "9388112", "NEVA TITAN", "UBNT2", 80, 11.8, "UST-LUGA"
        ));

        List<DetectionEvent> encounters = engine.searchEncounters(null, null);
        assertTrue(encounters.stream().anyMatch(e -> e.detectionType() == DetectionType.POTENTIAL_ENCOUNTER));
        assertTrue(encounters.stream().anyMatch(e -> e.detectionType() == DetectionType.DRAUGHT_CHANGE));
        // Verify tug was completely ignored in encounters
        assertTrue(encounters.stream().noneMatch(e -> tug.equals(e.mmsi()) || tug.equals(e.secondaryMmsi())));
    }

    @Test
    void shouldPreserveConflictingStaticReportsInHistory() {
        Instant t0 = Instant.parse("2026-11-10T12:00:00Z");
        String mmsi = "636091880";

        engine.processMessage(NormalizedAisMessage.staticReport(
                mmsi, t0, AisSource.AISSTREAM, false,
                "9274511", "CAUCASUS EXPRESS", "5LBM9", 82, 12.0, "ISTANBUL"
        ));
        engine.processMessage(NormalizedAisMessage.staticReport(
                mmsi, t0.plus(Duration.ofMinutes(20)), AisSource.AISSTREAM, false,
                "9299999", "VOLGA TRADER", "5LBM9", 82, 12.0, "CONSTANTA"
        ));

        VesselTrack track = engine.getTrack(mmsi).orElseThrow();
        assertTrue(track.hasIdentityConflict());
        assertEquals(2, track.staticHistory().size());
        assertEquals("CAUCASUS EXPRESS", track.staticHistory().get(0).vesselName());
        assertEquals("VOLGA TRADER", track.staticHistory().get(1).vesselName());
    }
}
