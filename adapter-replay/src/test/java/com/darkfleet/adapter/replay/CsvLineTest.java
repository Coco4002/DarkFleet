package com.darkfleet.adapter.replay;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvLineTest {

    @Test
    void splitsPlainFieldsAndKeepsEmptyOnes() {
        assertEquals(List.of("a", "", "c", ""), CsvLine.split("a,,c,"));
    }

    @Test
    void quotedFieldsMayContainCommasAndDoubledQuotes() {
        assertEquals(List.of("x", "ESBJERG,DK", "say \"hi\"", "y"), CsvLine.split("x,\"ESBJERG,DK\",\"say \"\"hi\"\"\",y"));
    }

    @Test
    void ignoresWindowsLineEnding() {
        assertEquals(List.of("a", "b"), CsvLine.split("a,b\r"));
    }
}
