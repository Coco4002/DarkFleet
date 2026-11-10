package com.darkfleet.common.spatial;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.CorridorRegion;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class SpatialEngineTest {

    @Test
    void shouldEncodeAndDecodeH3CellsConsistently() {
        double lat = 59.85;
        double lon = 25.40;
        String cell = SpatialEngine.toH3Cell(lat, lon);
        assertNotNull(cell);
        assertTrue(cell.startsWith("861f_"));

        double[] center = SpatialEngine.cellCenter(cell);
        assertEquals(lat, center[0], 0.08);
        assertEquals(lon, center[1], 0.15);
        assertEquals(7, SpatialEngine.cellAndNeighbors(lat, lon).size());
    }

    @Test
    void shouldDetectPortsAndAnchoragesCorrectly() {
        // Ust-Luga terminal
        assertTrue(SpatialEngine.isNearPortOrAnchorage(59.682, 28.405));
        // Open water in the middle of the Gulf of Finland
        assertFalse(SpatialEngine.isNearPortOrAnchorage(59.750, 25.800));
    }

    @Test
    void shouldClassifyCorridorsAndShipTypes() {
        assertEquals(CorridorRegion.BALTIC_CORRIDOR, CorridorRegion.classify(59.80, 25.20));
        assertEquals(CorridorRegion.BOSPHORUS_BLACK_SEA, CorridorRegion.classify(41.20, 29.10));

        assertTrue(NormalizedAisMessage.isCargoOrTankerType(80));
        assertTrue(NormalizedAisMessage.isCargoOrTankerType(74));
        assertFalse(NormalizedAisMessage.isCargoOrTankerType(52));
        assertTrue(NormalizedAisMessage.isTugOrPilotType(52));
        assertTrue(NormalizedAisMessage.isTugOrPilotType(50));

        NormalizedAisMessage msg = NormalizedAisMessage.positionReport(
                "273456780",
                Instant.parse("2026-11-10T08:00:00Z"),
                59.80, 25.20, 11.4, 265.0, 264.0, 0,
                AisSource.DIGITRAFFIC,
                false
        );
        assertTrue(msg.hasValidCoordinates());
        assertTrue(msg.dedupKey().startsWith("273456780:DIGITRAFFIC:POSITION_REPORT:"));
    }
}
