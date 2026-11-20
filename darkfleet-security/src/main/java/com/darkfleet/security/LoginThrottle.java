package com.darkfleet.security;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Slows down password guessing: after {@link #MAX_FAILURES} wrong passwords in a row for a username (or
 * {@link #MAX_IP_FAILURES} from one client IP across usernames) logins are refused for {@link #LOCK}.
 * Kept in memory per instance (a restart clears it), which is enough for the handful of accounts this
 * service has. Unknown usernames are counted too, so the answer never reveals whether an account exists.
 */
public class LoginThrottle {

    static final int MAX_FAILURES = 5;
    static final int MAX_IP_FAILURES = 15;
    static final Duration LOCK = Duration.ofMinutes(1);
    private static final int MAX_TRACKED = 10_000;

    private record State(int failures, Instant lockedUntil) {
    }

    private final Clock clock;
    private final ConcurrentHashMap<String, State> states = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, State> ipStates = new ConcurrentHashMap<>();

    public LoginThrottle(Clock clock) {
        this.clock = clock;
    }

    public boolean locked(String username) {
        return locked(username, null);
    }

    public boolean locked(String username, String clientIp) {
        return isLocked(states.get(key(username)))
                || (clientIp != null && !clientIp.isBlank() && isLocked(ipStates.get(clientIp)));
    }

    public void failed(String username) {
        failed(username, null);
    }

    public void failed(String username, String clientIp) {
        recordFailure(states, key(username), MAX_FAILURES);
        if (clientIp != null && !clientIp.isBlank()) {
            recordFailure(ipStates, clientIp, MAX_IP_FAILURES);
        }
    }

    public void succeeded(String username) {
        succeeded(username, null);
    }

    public void succeeded(String username, String clientIp) {
        states.remove(key(username));
        if (clientIp != null && !clientIp.isBlank()) {
            ipStates.remove(clientIp);
        }
    }

    private boolean isLocked(State s) {
        return s != null && s.lockedUntil() != null && s.lockedUntil().isAfter(clock.instant());
    }

    private void recordFailure(ConcurrentHashMap<String, State> map, String key, int maxFailures) {
        if (map.size() > MAX_TRACKED) {
            Instant now = clock.instant();
            map.values().removeIf(s -> s.lockedUntil() == null || !s.lockedUntil().isAfter(now));
        }
        map.compute(key, (k, s) -> {
            int failures = (s == null ? 0 : s.failures()) + 1;
            if (failures >= maxFailures) {
                return new State(0, clock.instant().plus(LOCK));
            }
            return new State(failures, s == null ? null : s.lockedUntil());
        });
    }

    private static String key(String username) {
        return username.toLowerCase(Locale.ROOT);
    }
}
