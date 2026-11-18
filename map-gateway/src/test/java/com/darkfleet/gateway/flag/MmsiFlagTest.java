package com.darkfleet.gateway.flag;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class MmsiFlagTest {

    @Test
    void resolvesBalticAndGlobalShippingRegisters() {
        MmsiFlag.Flag finland = MmsiFlag.resolve("230000001");
        assertNotNull(finland);
        assertEquals("FI", finland.code());
        assertEquals("Finland", finland.country());

        MmsiFlag.Flag russia = MmsiFlag.resolve("273456789");
        assertNotNull(russia);
        assertEquals("RU", russia.code());
        assertEquals("Russia", russia.country());

        MmsiFlag.Flag panama = MmsiFlag.resolve("351123456");
        assertNotNull(panama);
        assertEquals("PA", panama.code());
        assertEquals("Panama", panama.country());

        MmsiFlag.Flag cameroon = MmsiFlag.resolve("613000001");
        assertNotNull(cameroon);
        assertEquals("CM", cameroon.code());
        assertEquals("Cameroon", cameroon.country());
    }

    @Test
    void rejectsNonShipAndMalformedMmsis() {
        assertNull(MmsiFlag.resolve(null));
        assertNull(MmsiFlag.resolve("12345"));
        assertNull(MmsiFlag.resolve("002300001")); // coast station (00MIDXXXX)
        assertNull(MmsiFlag.resolve("111230001")); // SAR aircraft (111MIDXXX)
        assertNull(MmsiFlag.resolve("992300001")); // aid to navigation (99MIDXXXX)
        assertNull(MmsiFlag.resolve("23000A001"));
    }
}
