package com.darkfleet.intelligence.sanctions;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * A small RFC 4180 reader: comma-separated, fields optionally in double quotes, a quote inside quotes
 * written twice, line breaks allowed inside quotes, CRLF or LF line ends.
 */
final class Csv {

    private final Reader in;
    private int peeked = -2;

    Csv(Reader in) {
        this.in = in;
    }

    /** @return the next record's fields, or null at the end of the input */
    List<String> next() throws IOException {
        if (peek() == -1) {
            return null;
        }
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        boolean wasQuoted = false;
        while (true) {
            int c = read();
            if (quoted) {
                if (c == -1) {
                    throw new IOException("Unterminated quoted field");
                } else if (c == '"') {
                    if (peek() == '"') {
                        read();
                        field.append('"');
                    } else {
                        quoted = false;
                    }
                } else {
                    field.append((char) c);
                }
            } else if (c == '"' && field.isEmpty() && !wasQuoted) {
                quoted = true;
                wasQuoted = true;
            } else if (c == ',') {
                fields.add(field.toString());
                field.setLength(0);
                wasQuoted = false;
            } else if (c == '\r' || c == '\n' || c == -1) {
                if (c == '\r' && peek() == '\n') {
                    read();
                }
                fields.add(field.toString());
                return fields;
            } else {
                field.append((char) c);
            }
        }
    }

    private int peek() throws IOException {
        if (peeked == -2) {
            peeked = in.read();
        }
        return peeked;
    }

    private int read() throws IOException {
        int c = peek();
        peeked = -2;
        return c;
    }
}
