package com.darkfleet.detection;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.CoverageCell;
import com.darkfleet.detection.support.Grid;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoverageTest {

    private static final Instant START = Instant.parse("2026-11-10T10:00:00Z");

    @Test
    void publishesEachCompletedHourWithTheTotalsOfTheWindow() {
        try (Harness h = new Harness(DetectionMode.LIVE, START, Duration.ofMinutes(10))) {
            String cell = Grid.cell(59.9, 26.0);
            // hour 10: two vessels, one of them stopped; both feeds
            h.position("273600001", AisSource.AISSTREAM, START.plusSeconds(60), 59.9, 26.0, 10.0, 90.0, 0);
            h.position("273600001", AisSource.DIGITRAFFIC, START.plusSeconds(60), 59.9, 26.0, 10.0, 90.0, 0);
            h.position("273600002", AisSource.AISSTREAM, START.plusSeconds(120), 59.9, 26.0, 0.1, null, 1);
            // hour 11: one vessel
            h.position("273600001", AisSource.AISSTREAM, START.plus(Duration.ofMinutes(70)), 59.9, 26.0, 10.0, 90.0, 0);

            h.advance(Duration.ofMinutes(69));
            assertTrue(h.coverage().isEmpty(), "hour 10 still within its grace period");

            h.advance(Duration.ofMinutes(2));
            List<CoverageCell> published = h.coverage();
            assertEquals(1, published.size());
            CoverageCell hour10 = published.getFirst();
            assertEquals(cell, hour10.h3Cell());
            assertEquals(START, hour10.hourStart());
            assertEquals(2, hour10.positionsBySource().get(AisSource.AISSTREAM));
            assertEquals(2, hour10.vessels());
            assertEquals(1, hour10.stoppedVessels());

            h.advance(Duration.ofMinutes(60));
            CoverageCell hour11 = h.coverage().getFirst();
            assertEquals(START.plus(Duration.ofHours(1)), hour11.hourStart());
            assertEquals(1, hour11.positionsBySource().get(AisSource.AISSTREAM));
            assertEquals(3, hour11.windowPositions(AisSource.AISSTREAM));
            assertEquals(1, hour11.windowPositions(AisSource.DIGITRAFFIC));
            assertEquals(1, hour11.windowMaxStoppedVessels());

            // a quiet hour is still published, so the window totals stay current; a late report isn't counted
            h.position("273600003", AisSource.AISSTREAM, START.plusSeconds(300), 59.9, 26.0, 10.0, 90.0, 0);
            h.advance(Duration.ofMinutes(60));
            CoverageCell hour12 = h.coverage().getFirst();
            assertTrue(hour12.positionsBySource().isEmpty());
            assertEquals(3, hour12.windowPositions(AisSource.AISSTREAM));

            // once the window holds no traffic the cell drops out, and stays out
            h.advance(Duration.ofHours(24));
            List<CoverageCell> rest = h.coverage();
            assertEquals(START.plus(Duration.ofHours(24)), rest.getLast().hourStart(), "last window (24 h ending with its hour) still holding hour 11");
            h.position("273600004", AisSource.AISSTREAM, START.plus(Duration.ofMinutes(30)), 59.9, 26.0, 10.0, 90.0, 0);
            h.advance(Duration.ofHours(2));
            assertTrue(h.coverage().isEmpty(), "a report for a long-completed hour starts nothing new");
        }
    }
}
