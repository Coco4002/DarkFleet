package com.darkfleet.common.model;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * AIS sends the ETA as month, day, hour and minute in UTC, without a year. This resolves it to the
 * first matching moment no earlier than about a month before the message, so an ETA in early
 * January sent in late December lands in the next year.
 */
public final class AisEta {

    private AisEta() {
    }

    /**
     * @return the ETA, or null when AIS marks it as not available (month 0, day 0, hour 24, minute 60)
     *         or the values don't form a date
     */
    public static Instant resolve(int month, int day, int hour, int minute, Instant reportedAt) {
        if (month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
            return null;
        }
        ZonedDateTime reference = reportedAt.atZone(ZoneOffset.UTC);
        Instant earliest = reportedAt.minusSeconds(31L * 24 * 3600);
        for (int year = reference.getYear() - 1; year <= reference.getYear() + 1; year++) {
            try {
                Instant candidate = LocalDateTime.of(year, month, day, hour, minute).toInstant(ZoneOffset.UTC);
                if (!candidate.isBefore(earliest)) {
                    return candidate;
                }
            } catch (DateTimeException invalidDate) {
                // e.g. 29 February in a non-leap year: try the next year
            }
        }
        return null;
    }

    /**
     * Decodes the packed 20-bit ETA field used by Digitraffic (month bits 16-19, day 11-15,
     * hour 6-10, minute 0-5).
     */
    public static Instant fromPacked(long packed, Instant reportedAt) {
        if (packed <= 0 || packed > 0xFFFFF) {
            return null; // not set, or not a 20-bit AIS value
        }
        int month = (int) ((packed >> 16) & 0xF);
        int day = (int) ((packed >> 11) & 0x1F);
        int hour = (int) ((packed >> 6) & 0x1F);
        int minute = (int) (packed & 0x3F);
        return resolve(month, day, hour, minute, reportedAt);
    }
}
