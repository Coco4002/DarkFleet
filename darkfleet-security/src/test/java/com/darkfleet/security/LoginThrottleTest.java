package com.darkfleet.security;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoginThrottleTest {

    @Test
    void locksByUsernameAndByClientIpAcrossUsernames() {
        Clock clock = Clock.fixed(Instant.parse("2026-11-20T12:00:00Z"), ZoneOffset.UTC);
        LoginThrottle throttle = new LoginThrottle(clock);

        // 5 failures for the same username locks that username
        for (int i = 0; i < LoginThrottle.MAX_FAILURES; i++) {
            assertFalse(throttle.locked("analyst", "198.51.100.1"));
            throttle.failed("analyst", "198.51.100.1");
        }
        assertTrue(throttle.locked("analyst", "198.51.100.2"));

        // Spraying failures across distinct usernames from the same IP locks that IP after MAX_IP_FAILURES
        String attackerIp = "203.0.113.99";
        for (int i = 0; i < LoginThrottle.MAX_IP_FAILURES; i++) {
            String user = "user-" + i;
            assertFalse(throttle.locked(user, attackerIp));
            throttle.failed(user, attackerIp);
        }
        assertTrue(throttle.locked("another-user", attackerIp));
        assertFalse(throttle.locked("another-user", "198.51.100.50"));
    }
}
