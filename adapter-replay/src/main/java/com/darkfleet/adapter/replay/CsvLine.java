package com.darkfleet.adapter.replay;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits one CSV line into fields: comma-separated, fields may be wrapped in double quotes
 * (which may contain commas), and a doubled quote inside a quoted field is a literal quote.
 * DMA files never break a field across lines, so one line is one record.
 */
final class CsvLine {

    private CsvLine() {
    }

    static List<String> split(String line) {
        List<String> fields = new ArrayList<>(32);
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    field.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                fields.add(field.toString());
                field.setLength(0);
            } else if (c != '\r') {
                field.append(c);
            }
        }
        fields.add(field.toString());
        return fields;
    }
}
