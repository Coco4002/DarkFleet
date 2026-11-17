package com.darkfleet.intelligence;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.common.model.TrackPoint;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Replay mode reads the replay.* topics and keeps everything in the "replay" schema. */
@SpringBootTest
@ActiveProfiles("replay")
class ReplayModeTest extends IntegrationTest {

    @Test
    void replayedDataStaysInTheReplaySchema() {
        String mmsi = "219000001";
        Instant time = Instant.parse("2026-11-11T05:00:00Z");
        Detection gap = new Detection("gap:" + mmsi + ":1", DetectionType.UNEXPLAINED_GAP, DetectionStatus.OPEN, 1,
                mmsi, null, time, null, 57.5, 10.6, "8609a4c57ffffff", 0, Map.of());
        TrackPoint point = IngestTest.point(mmsi, time);

        send(DarkFleetTopics.output(DarkFleetTopics.DETECTIONS, true), mmsi, gap);
        send(DarkFleetTopics.output(DarkFleetTopics.TRACK_POINTS, true), mmsi, point);

        awaitCount(1, "SELECT count(*) FROM replay.detection WHERE detection_id = ?", gap.detectionId());
        awaitCount(1, "SELECT count(*) FROM replay.track_point WHERE mmsi = ? AND time = ?", mmsi, Timestamp.from(time));

        assertEquals("replay", jdbc.queryForObject(
                "SELECT string_agg(schemaname, ',') FROM pg_tables WHERE tablename = 'track_point_20261111'", String.class));
        // the live tables, if the live service already created them, got nothing
        if (jdbc.queryForObject("SELECT to_regclass('public.detection') IS NOT NULL", Boolean.class)) {
            assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM public.detection WHERE detection_id = ?",
                    Long.class, gap.detectionId()));
        }
    }
}
