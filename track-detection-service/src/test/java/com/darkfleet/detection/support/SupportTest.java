package com.darkfleet.detection.support;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SupportTest {

    @Test
    void imoCheckDigit() {
        assertTrue(Imo.isValid("9074729"));
        assertTrue(Imo.isValid("9312456"));
        assertFalse(Imo.isValid("9312457"));
        assertFalse(Imo.isValid("0000000"), "passes the check digit but is no IMO number");
        assertFalse(Imo.isValid("931245"));
        assertFalse(Imo.isValid(null));
    }

    @Test
    void distancesAndDestinations() {
        // one degree of latitude is about 111.2 km
        assertEquals(111_195, Geo.distanceMeters(59.0, 25.0, 60.0, 25.0), 50);
        double[] east = Geo.destination(59.9, 26.0, 90, 1000);
        assertEquals(1000, Geo.distanceMeters(59.9, 26.0, east[0], east[1]), 0.5);
        assertEquals(20, Geo.courseDifference(350, 10), 1e-9);
    }

    @Test
    void cellsWithinStartWithTheOwnCellAndReachNeighbours() {
        assertEquals(15, Grid.cell(59.9, 26.0).length());
        assertEquals(Grid.cell(59.9, 26.0), Grid.cellsWithin(59.9, 26.0, 500).iterator().next());
        assertTrue(Grid.cellsWithin(59.9, 26.0, 10_000).size() > 1);
    }

    @Test
    void sightingsConflictOnlyWhenPeriodsOverlap() {
        Instant t = Instant.parse("2026-11-10T10:00:00Z");
        Sightings s = new Sightings();
        s.record("A", t);
        s.record("A", t.plusSeconds(600));
        s.record("B", t.plusSeconds(1200));
        assertTrue(s.conflicting().isEmpty(), "A stopped before B started");
        s.record("A", t.plusSeconds(1800));
        assertEquals(java.util.List.of("A", "B"), s.conflicting());
        s.forgetBefore(t.plusSeconds(1500));
        assertTrue(s.conflicting().isEmpty());
    }
}
