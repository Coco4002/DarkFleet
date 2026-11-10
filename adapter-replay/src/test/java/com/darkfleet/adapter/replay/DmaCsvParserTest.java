package com.darkfleet.adapter.replay;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DmaCsvParserTest {

    private final DmaCsvParser parser = new DmaCsvParser();

    @Test
    void shouldParseDmaCsvLinesAndMarkHistorical() {
        String csv = """
                # Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Draught,Destination
                10/11/2026 07:30:00,Class A,273511200,56.9500,11.4500,Under way using engine,0.0,1.2,180.0,180,9388112,UBNT2,NEVA TITAN,Tanker,14.6,PORT SAID
                """;

        List<NormalizedAisMessage> messages = parser.parseCsvContent(csv);
        assertEquals(2, messages.size());

        NormalizedAisMessage staticMsg = messages.get(0);
        assertEquals(AisMessageType.STATIC_VOYAGE, staticMsg.messageType());
        assertEquals("273511200", staticMsg.mmsi());
        assertEquals("9388112", staticMsg.imoNumber());
        assertEquals("NEVA TITAN", staticMsg.vesselName());
        assertEquals(80, staticMsg.shipType());
        assertEquals(14.6, staticMsg.draughtMeters(), 0.01);
        assertTrue(staticMsg.historical());
        assertEquals(AisSource.DMA_REPLAY, staticMsg.source());

        NormalizedAisMessage posMsg = messages.get(1);
        assertEquals(AisMessageType.POSITION_REPORT, posMsg.messageType());
        assertEquals(56.9500, posMsg.latitude(), 0.0001);
        assertEquals(11.4500, posMsg.longitude(), 0.0001);
        assertTrue(posMsg.historical());
    }
}
