package com.darkfleet.adapter.replay;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Uses real rows from the DMA file of 11 November 2026 (src/test/resources/dma/aisdk-sample.csv).
 */
class DmaCsvParserTest {

    private static List<NormalizedAisMessage> messages;

    @BeforeAll
    static void parseSample() throws IOException {
        List<String> lines = readSample();
        DmaCsvParser parser = DmaCsvParser.fromHeader(lines.getFirst());
        messages = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            messages.addAll(parser.parse(line));
        }
        assertEquals(21, parser.rows());
        assertEquals(4, parser.skippedNotVessel(), "2 base stations + 2 aids to navigation");
        assertEquals(0, parser.skippedInvalid());
    }

    static List<String> readSample() throws IOException {
        try (InputStream in = DmaCsvParserTest.class.getResourceAsStream("/dma/aisdk-sample.csv")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList();
        }
    }

    private static List<NormalizedAisMessage> of(String mmsi, AisMessageType type) {
        return messages.stream().filter(m -> m.mmsi().equals(mmsi) && m.messageType() == type).toList();
    }

    @Test
    void skipsBaseStationsAndAidsToNavigation() {
        assertTrue(messages.stream().noneMatch(m -> m.mmsi().startsWith("219006") || m.mmsi().startsWith("992")));
    }

    @Test
    void everyMessageIsHistoricalDmaReplay() {
        assertTrue(messages.stream().allMatch(m -> m.historical() && m.source() == AisSource.DMA_REPLAY));
    }

    @Test
    void staticRowWithPlaceholderPositionYieldsOneStaticReportAndNoPosition() {
        // KMARIN REASON: the same static row three times, position 91/0
        assertEquals(0, of("235108534", AisMessageType.POSITION_REPORT).size());
        List<NormalizedAisMessage> statics = of("235108534", AisMessageType.STATIC_VOYAGE);
        assertEquals(1, statics.size(), "repeated static data is emitted once");
        NormalizedAisMessage s = statics.getFirst();
        assertEquals("9683087", s.imoNumber());
        assertEquals("KMARIN REASON", s.vesselName());
        assertEquals("2ICI7", s.callSign());
        assertEquals(80, s.shipType());
        assertEquals(8.8, s.draughtMeters(), 0.001);
        assertEquals("DK SKA", s.destination());
        assertEquals(250, s.lengthMeters(), "A + B = 202 + 48");
        assertEquals(44, s.beamMeters(), "C + D = 18 + 26");
        assertEquals(Instant.parse("2026-11-11T00:01:00Z"), s.eta());
        assertEquals(Instant.parse("2026-11-11T00:00:02Z"), s.eventTime());
    }

    @Test
    void quotedDestinationKeepsItsComma() {
        NormalizedAisMessage s = of("219032994", AisMessageType.STATIC_VOYAGE).getFirst();
        assertEquals("ESBJERG,DK", s.destination());
        assertEquals(Instant.parse("2027-10-30T16:15:00Z"), s.eta());
        assertEquals(70, s.shipType());
    }

    @Test
    void positionRowsMapNavigationStatusAndCourse() {
        List<NormalizedAisMessage> positions = of("265074000", AisMessageType.POSITION_REPORT);
        assertEquals(3, positions.size());
        NormalizedAisMessage p = positions.getFirst();
        assertEquals(57.617815, p.latitude(), 1e-6);
        assertEquals(11.761557, p.longitude(), 1e-6);
        assertEquals(7, p.navStatus(), "Engaged in fishing");
        assertEquals(235.0, p.headingDegrees(), 0.001);
        assertEquals(216.9, p.cogDegrees(), 0.001);
        assertEquals(0.0, p.sogKnots(), 0.001);
        // static data repeated on the position rows is unchanged, so no second static report
        assertEquals(1, of("265074000", AisMessageType.STATIC_VOYAGE).size());
    }

    @Test
    void tankerUnderWayWithStaticDataOnThePositionRow() {
        NormalizedAisMessage p = of("563326000", AisMessageType.POSITION_REPORT).getFirst();
        assertEquals(4, p.navStatus(), "Constrained by her draught");
        assertEquals(12.5, p.sogKnots(), 0.001);
        assertEquals(1, of("563326000", AisMessageType.STATIC_VOYAGE).size());
        assertEquals(80, of("563326000", AisMessageType.STATIC_VOYAGE).getFirst().shipType());
    }

    @Test
    void missingCourseIsNullNotZero() {
        NormalizedAisMessage classB = of("219029338", AisMessageType.POSITION_REPORT).getFirst();
        assertNull(classB.cogDegrees());
        assertNull(classB.headingDegrees());
        assertNull(classB.navStatus(), "Unknown value");
        assertEquals(0, of("219029338", AisMessageType.STATIC_VOYAGE).size(), "no static data on this row");
    }

    @Test
    void columnsAreFoundByNameNotPosition() {
        DmaCsvParser parser = DmaCsvParser.fromHeader("MMSI,Timestamp,Latitude,Longitude,Type of mobile,SOG");
        List<NormalizedAisMessage> out = parser.parse("265074000,11/11/2026 00:00:36,57.6,11.7,Class A,4.2");
        assertEquals(1, out.size());
        assertEquals(4.2, out.getFirst().sogKnots(), 0.001);
    }

    @Test
    void rejectsFilesThatAreNotDmaAis() {
        assertThrows(IllegalArgumentException.class, () -> DmaCsvParser.fromHeader("id,name,price"));
    }

    @Test
    void skipsRowsWithBadMmsiOrTimestamp() {
        DmaCsvParser parser = DmaCsvParser.fromHeader("Timestamp,Type of mobile,MMSI,Latitude,Longitude");
        assertTrue(parser.parse("11/11/2026 00:00:00,Class A,12345,57.6,11.7").isEmpty());
        assertTrue(parser.parse("2026-11-11 00:00,Class A,265074000,57.6,11.7").isEmpty());
        assertEquals(2, parser.skippedInvalid());
    }
}
