package com.darkfleet.intelligence.sanctions;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

/**
 * @param url    where the maritime CSV is downloaded from (http(s), or classpath:/file: for tests);
 *               blank turns the automatic import off
 * @param maxAge a list older than this is imported again (checked hourly and at start-up)
 * @param minShare an import with fewer vessels than this share of the previous import is refused as a
 *               probably truncated file, keeping the previous list
 */
@ConfigurationProperties("darkfleet.intelligence.sanctions")
public record SanctionsProperties(String url, Duration maxAge, double minShare) {

    public SanctionsProperties {
        Objects.requireNonNull(maxAge, "darkfleet.intelligence.sanctions.max-age is required");
        if (minShare < 0 || minShare > 1) {
            throw new IllegalArgumentException("darkfleet.intelligence.sanctions.min-share must be between 0 and 1");
        }
    }

    public boolean enabled() {
        return url != null && !url.isBlank();
    }
}
