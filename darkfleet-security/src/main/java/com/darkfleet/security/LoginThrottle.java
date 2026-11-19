package com.darkfleet.security;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Slows down password guessing: after {@link #MAX_FAILURES} wrong passwords in a row for a username the
 * account refuses logins for {@link #LOCK}. Kept in memory per instance (a restart clears it), which is
 * enough for the handful of accounts this service has. Unknown usernames are counted too, so the
 * answer never reveals whether an account exists.
 */
public class LoginThrottle {

    static final int MAX_FAILURES = 5;
    static final Duration LOCK = Duration.ofMinutes(1);
    private static final int MAX_TRACKED = 10_000;

    private record State(int failures, Instant lockedUntil) {
    }

    private final Clock clock;
    private final ConcurrentHashMap<String, State> states = new ConcurrentHashMap<>();

    public LoginThrottle(Clock clock) {
        this.clock = clock;
    }

    public boolean locked(String username) {
        State s = states.get(key(username));
        return s != null && s.lockedUntil() != null && s.lockedUntil().isAfter(clock.instant());
    }

    public void failed(String username) {
        if (states.size() > MAX_TRACKED) {
            Instant now = clock.instant();
            states.values().removeIf(s -> s.lockedUntil() == null || !s.lockedUntil().isAfter(now));
        }
        states.compute(key(username), (k, s) -> {
            int failures = (s == null ? 0 : s.failures()) + 1;
            if (failures >= MAX_FAILURES) {
                return new State(0, clock.instant().plus(LOCK));
            }
            return new State(failures, s == null ? null : s.lockedUntil());
        });
    }

    public void succeeded(String username) {
        states.remove(key(username));
    }

    private static String key(String username) {
        return username.toLowerCase(Locale.ROOT);
    }
}
