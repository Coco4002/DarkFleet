package com.darkfleet.intelligence.retention;

import com.darkfleet.intelligence.ingest.TrackPartitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Keeps the database to its agreed size. Hourly:
 * <ul>
 *   <li>Daily track partitions older than {@code trackDays} are dropped (instant, unlike deleting millions
 *       of rows). Just before, the points within {@code keepAroundDetection} of a detection of their
 *       vessel are copied to {@code track_point_kept}, in the same transaction as the drop.</li>
 *   <li>Coverage rows older than {@code coverageDays} are deleted.</li>
 * </ul>
 * Detections, cases, proposals and the audit log are kept.
 */
@Component
public class RetentionJob {

    private static final Logger log = LoggerFactory.getLogger(RetentionJob.class);
    private static final DateTimeFormatter DAY = DateTimeFormatter.BASIC_ISO_DATE;

    private static final String KEEP_NEAR_DETECTIONS = """
            INSERT INTO track_point_kept
            SELECT t.* FROM %s t
            WHERE EXISTS (
                SELECT 1 FROM detection d
                WHERE (d.mmsi = t.mmsi OR d.other_mmsi = t.mmsi)
                  AND t.time >= d.started_at - make_interval(secs => ?)
                  AND t.time <= coalesce(d.ended_at, now()) + make_interval(secs => ?))
            ON CONFLICT (mmsi, time) DO NOTHING""";

    private final RetentionProperties properties;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final TrackPartitions partitions;
    private final Clock clock;

    public RetentionJob(RetentionProperties properties, JdbcTemplate jdbc, TransactionTemplate transaction,
                        TrackPartitions partitions, Clock clock) {
        this.properties = properties;
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.partitions = partitions;
        this.clock = clock;
    }

    @Scheduled(initialDelayString = "PT2M", fixedDelayString = "PT1H")
    void scheduled() {
        if (properties.enabled()) {
            run();
        }
    }

    /** One pass; returns the days whose partitions were dropped. */
    public List<LocalDate> run() {
        LocalDate today = LocalDate.ofInstant(clock.instant(), ZoneOffset.UTC);
        LocalDate oldestKept = today.minusDays(properties.trackDays());
        List<LocalDate> dropped = partitionDays().stream().filter(day -> day.isBefore(oldestKept)).toList();
        for (LocalDate day : dropped) {
            dropDay(day);
        }
        int coverage = jdbc.update("DELETE FROM coverage_cell WHERE hour_start < ?",
                java.sql.Timestamp.from(today.minusDays(properties.coverageDays()).atStartOfDay(ZoneOffset.UTC).toInstant()));
        if (!dropped.isEmpty() || coverage > 0) {
            log.info("Retention: dropped track days {}, deleted {} coverage rows", dropped, coverage);
        }
        return dropped;
    }

    private void dropDay(LocalDate day) {
        String partition = TrackPartitions.partitionName(day);
        long seconds = properties.keepAroundDetection().toSeconds();
        Integer kept = transaction.execute(status -> {
            // the same lock as partition creation: a point for this day arriving now waits, then recreates it
            jdbc.query("SELECT pg_advisory_xact_lock(hashtext('track_point partitions ' || current_schema()))", rs -> { });
            // waits for inserts into the day still in flight, so the copy below sees them, and holds off new ones
            jdbc.execute("LOCK TABLE " + partition + " IN ACCESS EXCLUSIVE MODE");
            int rows = jdbc.update(KEEP_NEAR_DETECTIONS.formatted(partition), seconds, seconds);
            jdbc.execute("DROP TABLE " + partition);
            return rows;
        });
        partitions.forget(day);
        log.info("Dropped track partition {} after keeping {} points near detections", partition, kept);
    }

    /** Days that have a track partition in this service's schema. */
    private List<LocalDate> partitionDays() {
        return jdbc.queryForList("""
                SELECT c.relname FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = p.relnamespace
                WHERE p.relname = 'track_point' AND n.nspname = current_schema()""", String.class)
                .stream()
                .map(name -> name.substring("track_point_".length()))
                .flatMap(suffix -> {
                    try {
                        return java.util.stream.Stream.of(LocalDate.parse(suffix, DAY));
                    } catch (DateTimeParseException e) {
                        return java.util.stream.Stream.empty();
                    }
                })
                .sorted()
                .toList();
    }
}
