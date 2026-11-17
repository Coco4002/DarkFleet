package com.darkfleet.intelligence.ingest;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates the daily partitions of {@code track_point} as points for new days arrive. Points can belong
 * to any day: live data is "today", a replay brings the days it replays.
 */
@Component
public class TrackPartitions {

    private static final DateTimeFormatter DAY = DateTimeFormatter.BASIC_ISO_DATE;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    /** Days known to have a partition; only ever a cache, the database decides. */
    private final Set<LocalDate> known = ConcurrentHashMap.newKeySet();

    public TrackPartitions(JdbcTemplate jdbc, TransactionTemplate transaction) {
        this.jdbc = jdbc;
        this.transaction = transaction;
    }

    /** Makes sure every day these times fall on has its partition. Commits on its own. */
    public void ensure(Collection<Instant> times) {
        times.stream()
                .map(time -> LocalDate.ofInstant(time, ZoneOffset.UTC))
                .distinct()
                .filter(day -> !known.contains(day))
                .forEach(this::create);
    }

    /** Forgets a day, e.g. after its partition was dropped. */
    public void forget(LocalDate day) {
        known.remove(day);
    }

    public static String partitionName(LocalDate day) {
        return "track_point_" + DAY.format(day);
    }

    private void create(LocalDate day) {
        transaction.executeWithoutResult(status -> {
            // one creator at a time: two sessions creating the same partition can fail even with IF NOT EXISTS
            jdbc.query("SELECT pg_advisory_xact_lock(hashtext('track_point partitions ' || current_schema()))", rs -> { });
            // built from a LocalDate only, never from message content
            jdbc.execute("CREATE TABLE IF NOT EXISTS " + partitionName(day) + " PARTITION OF track_point"
                    + " FOR VALUES FROM ('" + day + " 00:00:00+00') TO ('" + day.plusDays(1) + " 00:00:00+00')");
        });
        known.add(day);
    }
}
