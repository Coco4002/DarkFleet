package com.darkfleet.intelligence.retention;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

/**
 * @param enabled         off in replay mode, whose data is old by definition
 * @param trackDays       full days of track history kept; older daily partitions are dropped
 * @param keepAroundDetection track points this close to a detection of their vessel are kept anyway
 * @param coverageDays    days of hourly coverage kept
 */
@ConfigurationProperties("darkfleet.intelligence.retention")
public record RetentionProperties(boolean enabled, int trackDays, Duration keepAroundDetection, int coverageDays) {

    public RetentionProperties {
        Objects.requireNonNull(keepAroundDetection, "darkfleet.intelligence.retention.keep-around-detection is required");
        if (trackDays < 1 || coverageDays < 1) {
            throw new IllegalArgumentException("Retention must keep at least one day");
        }
    }
}
