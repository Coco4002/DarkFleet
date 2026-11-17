package com.darkfleet.intelligence;

import com.darkfleet.intelligence.ingest.TrackPartitions;
import com.darkfleet.intelligence.retention.RetentionJob;
import com.darkfleet.intelligence.retention.RetentionProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

/**
 * Dropping expired track days while keeping the points around detections. Uses days in October 2026,
 * which no other test touches, and a clock set to 9 October.
 */
class RetentionTest extends IntegrationTest {

    private static final String MMSI = "273300001";

    @Autowired
    private TrackPartitions partitions;
    @Autowired
    private TransactionTemplate transaction;

    @Test
    void expiredDaysAreDroppedAfterKeepingThePointsNearDetections() throws Exception {
        Seed seed = new Seed(jdbc, partitions);
        seed.detection("retention:gap", "UNEXPLAINED_GAP", "CLOSED", MMSI, null, "2026-10-01T12:00:00Z",
                "2026-10-01T13:00:00Z", 59.0, 22.0, null);
        seed.trackPoints(MMSI, "cell",
                Instant.parse("2026-10-01T02:00:00Z"),   // 10 h before the gap: dropped
                Instant.parse("2026-10-01T07:00:00Z"),   // 5 h before: kept
                Instant.parse("2026-10-01T18:30:00Z"),   // 5.5 h after it ended: kept
                Instant.parse("2026-10-02T08:00:00Z"));  // a day still within retention
        seed.coverage("retention-cell", "2026-10-01T05:00:00Z");

        Clock ninthOfOctober = Clock.fixed(Instant.parse("2026-10-09T03:00:00Z"), ZoneOffset.UTC);
        var job = new RetentionJob(new RetentionProperties(true, 7, Duration.ofHours(6), 7), jdbc, transaction,
                partitions, ninthOfOctober);

        List<LocalDate> dropped = job.run();

        assertEquals(List.of(LocalDate.parse("2026-10-01")), dropped);
        assertNull(jdbc.queryForObject("SELECT to_regclass('track_point_20261001')::text", String.class));
        assertEquals(List.of(Instant.parse("2026-10-01T07:00:00Z"), Instant.parse("2026-10-01T18:30:00Z")),
                jdbc.queryForList("SELECT time FROM track_point_kept WHERE mmsi = ? ORDER BY time",
                        java.sql.Timestamp.class, MMSI).stream().map(java.sql.Timestamp::toInstant).toList());
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM track_point WHERE mmsi = ?", Long.class, MMSI));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM coverage_cell WHERE h3_cell = 'retention-cell'", Long.class));

        // the API still shows the kept points
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}/track?from=2026-10-01T00:00:00Z&to=2026-10-02T12:00:00Z", MMSI))
                .andExpect(jsonPath("$[*].time", contains("2026-10-01T07:00:00Z", "2026-10-01T18:30:00Z",
                        "2026-10-02T08:00:00Z")));

        // a late point for the dropped day recreates its partition instead of failing
        seed.trackPoints(MMSI, "cell", Instant.parse("2026-10-01T20:00:00Z"));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM track_point_20261001", Long.class));
    }
}
