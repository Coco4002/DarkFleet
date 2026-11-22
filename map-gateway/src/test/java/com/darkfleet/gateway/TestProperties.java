package com.darkfleet.gateway;

import java.time.Duration;

/** The gateway's settings as in application.yml, for tests that build the classes by hand. */
public final class TestProperties {

    private TestProperties() {
    }

    public static GatewayProperties gateway(boolean replay, String wikimediaApiUrl) {
        return new GatewayProperties(replay, Duration.ofHours(1), Duration.ofDays(7), Duration.ofDays(7),
                Duration.ofSeconds(5), Duration.ofMinutes(2), "http://localhost:8086", "test-gateway-key",
                Duration.ofSeconds(5), wikimediaApiUrl, Duration.ofSeconds(3), Duration.ofHours(24),
                120, 5);
    }
}
