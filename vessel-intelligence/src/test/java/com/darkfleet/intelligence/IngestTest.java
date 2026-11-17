package com.darkfleet.intelligence;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.CoverageCell;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The live consumers end to end: messages on the Kafka topics, rows in PostGIS. Each test uses its
 * own MMSIs. Messages with the same key share a partition and are processed in the order sent, so a
 * test that needs to know a message was processed waits for a later one with the same key.
 */
@SpringBootTest
class IngestTest extends IntegrationTest {

    private static final String CELL = "861126d37ffffff";
    private static final Instant T0 = Instant.parse("2026-11-11T02:00:00Z");

    @Test
    void detectionKeepsItsNewestVersion() {
        String mmsi = "230000001";
        Detection open = new Detection("gap:" + mmsi + ":1", DetectionType.UNEXPLAINED_GAP, DetectionStatus.OPEN, 1,
                mmsi, null, T0, null, 60.1, 25.0, CELL, 0, Map.of("gapMinutes", 60));
        Detection closed = open.closed(T0.plusSeconds(5400), Map.of("closeReason", "reported again"));
        // an identity conflict has no position; it also tells the test when the others were processed
        Detection last = new Detection("identity:" + mmsi + ":1", DetectionType.IDENTITY_INCONSISTENCY,
                DetectionStatus.OPEN, 1, mmsi, "230000002", T0, null, null, null, null, 0, Map.of());

        send(DarkFleetTopics.DETECTIONS, mmsi, closed);
        send(DarkFleetTopics.DETECTIONS, mmsi, open);   // delivered late
        send(DarkFleetTopics.DETECTIONS, mmsi, closed); // delivered twice
        send(DarkFleetTopics.DETECTIONS, mmsi, last);
        awaitCount(1, "SELECT count(*) FROM detection WHERE detection_id = ?", last.detectionId());

        Map<String, Object> gap = jdbc.queryForMap("""
                SELECT status, version, ended_at, evidence->>'closeReason' AS reason,
                       ST_Y(position) AS lat, ST_X(position) AS lon
                FROM detection WHERE detection_id = ?""", open.detectionId());
        assertEquals("CLOSED", gap.get("status"));
        assertEquals(2, gap.get("version"));
        assertEquals(T0.plusSeconds(5400), ((Timestamp) gap.get("ended_at")).toInstant());
        assertEquals("reported again", gap.get("reason"));
        assertEquals(60.1, (Double) gap.get("lat"), 1e-9);
        assertEquals(25.0, (Double) gap.get("lon"), 1e-9);

        Map<String, Object> identity = jdbc.queryForMap(
                "SELECT other_mmsi, position FROM detection WHERE detection_id = ?", last.detectionId());
        assertEquals("230000002", identity.get("other_mmsi"));
        assertNull(identity.get("position"));
    }

    @Test
    void trackPointsAreStoredOnceInTheirDaysPartition() {
        String mmsi = "230000011";
        TrackPoint beforeMidnight = point(mmsi, Instant.parse("2026-11-09T23:59:00Z"));
        TrackPoint afterMidnight = point(mmsi, Instant.parse("2026-11-10T00:01:00Z"));
        TrackPoint last = point(mmsi, Instant.parse("2026-11-10T00:02:00Z"));

        for (int i = 0; i < 2; i++) {
            send(DarkFleetTopics.TRACK_POINTS, mmsi, beforeMidnight);
            send(DarkFleetTopics.TRACK_POINTS, mmsi, afterMidnight);
        }
        send(DarkFleetTopics.TRACK_POINTS, mmsi, last);
        awaitCount(1, "SELECT count(*) FROM track_point WHERE mmsi = ? AND time = ?", mmsi,
                Timestamp.from(last.time()));

        assertEquals(3L, jdbc.queryForObject("SELECT count(*) FROM track_point WHERE mmsi = ?", Long.class, mmsi));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM track_point_20261109 WHERE mmsi = ?", Long.class, mmsi));
        assertEquals(2L, jdbc.queryForObject("SELECT count(*) FROM track_point_20261110 WHERE mmsi = ?", Long.class, mmsi));
    }

    @Test
    void vesselKeepsNewestDataAndEveryIdentityItBroadcast() {
        String mmsi = "230000021";
        Instant t1 = T0;
        Instant t2 = T0.plusSeconds(3600);
        Instant t3 = T0.plusSeconds(7200);
        VesselStatic renamed = staticReport(mmsi, t2, "NEW NAME", "ARCTIC", 250);
        VesselStatic olderName = staticReport(mmsi, t1, "OLD NAME", "ARCTIC", 250);
        VesselStatic newVoyage = staticReport(mmsi, t3, "NEW NAME", "PRIMORSK", null);

        send(DarkFleetTopics.VESSEL_STATIC, mmsi, renamed);
        send(DarkFleetTopics.VESSEL_STATIC, mmsi, olderName); // delivered late
        send(DarkFleetTopics.VESSEL_STATIC, mmsi, newVoyage);
        awaitCount(1, "SELECT count(*) FROM vessel WHERE mmsi = ? AND reported_at = ?", mmsi, Timestamp.from(t3));

        Map<String, Object> vessel = jdbc.queryForMap(
                "SELECT name, imo, destination, length_m FROM vessel WHERE mmsi = ?", mmsi);
        assertEquals("NEW NAME", vessel.get("name"));
        assertEquals("9074729", vessel.get("imo"));
        assertEquals("PRIMORSK", vessel.get("destination"));
        assertEquals(250, vessel.get("length_m"), "a field missing from a newer report keeps its value");

        assertEquals(2L, jdbc.queryForObject("SELECT count(*) FROM identity_report WHERE mmsi = ?", Long.class, mmsi));
        Map<String, Object> current = jdbc.queryForMap(
                "SELECT first_seen, last_seen FROM identity_report WHERE mmsi = ? AND name = 'NEW NAME'", mmsi);
        assertEquals(t2, ((Timestamp) current.get("first_seen")).toInstant());
        assertEquals(t3, ((Timestamp) current.get("last_seen")).toInstant());
        Map<String, Object> old = jdbc.queryForMap(
                "SELECT first_seen, last_seen FROM identity_report WHERE mmsi = ? AND name = 'OLD NAME'", mmsi);
        assertEquals(t1, ((Timestamp) old.get("first_seen")).toInstant());
        assertEquals(t1, ((Timestamp) old.get("last_seen")).toInstant());
    }

    @Test
    void laterCoverageRecordForTheSameHourWins() {
        String cell = "861126d2fffffff";
        send(DarkFleetTopics.COVERAGE_CELLS, cell, coverage(cell, 3));
        send(DarkFleetTopics.COVERAGE_CELLS, cell, coverage(cell, 5));
        awaitCount(1, "SELECT count(*) FROM coverage_cell WHERE h3_cell = ? AND vessels = 5", cell);

        Map<String, Object> row = jdbc.queryForMap("""
                SELECT count(*) OVER () AS rows, (positions_by_source->>'AISSTREAM')::int AS aisstream
                FROM coverage_cell WHERE h3_cell = ?""", cell);
        assertEquals(1L, row.get("rows"));
        assertEquals(40, row.get("aisstream"));
    }

    @Test
    void unreadableMessagesAreSkipped() {
        String mmsi = "230000031";
        Detection valid = new Detection("loitering:" + mmsi + ":1", DetectionType.LOITERING, DetectionStatus.OPEN, 1,
                mmsi, null, T0, null, 59.9, 24.5, CELL, 0, Map.of());

        sendRaw(DarkFleetTopics.DETECTIONS, mmsi, "not json".getBytes(StandardCharsets.UTF_8));
        sendRaw(DarkFleetTopics.DETECTIONS, mmsi, "{\"detectionId\":\"missing-fields\"}".getBytes(StandardCharsets.UTF_8));
        send(DarkFleetTopics.DETECTIONS, mmsi, valid);

        awaitCount(1, "SELECT count(*) FROM detection WHERE detection_id = ?", valid.detectionId());
        assertNotNull(jdbc.queryForObject("SELECT status FROM detection WHERE detection_id = ?", String.class,
                valid.detectionId()));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM detection WHERE detection_id = 'missing-fields'", Long.class));
    }

    @Test
    void messagesTheDatabaseRefusesAreSkippedWithoutHoldingUpTheOthers() {
        String mmsi = "230000041";
        // PostgreSQL text can't hold a NUL character; AIS text fields come from radio and may carry one
        VesselStatic refused = staticReport(mmsi, T0, "BAD\u0000NAME", "ARCTIC", 250);
        VesselStatic fine = staticReport(mmsi, T0.plusSeconds(60), "GOOD NAME", "ARCTIC", 250);

        sendRaw(DarkFleetTopics.VESSEL_STATIC, mmsi, "null".getBytes(StandardCharsets.UTF_8));
        send(DarkFleetTopics.VESSEL_STATIC, mmsi, refused);
        send(DarkFleetTopics.VESSEL_STATIC, mmsi, fine);

        awaitCount(1, "SELECT count(*) FROM vessel WHERE mmsi = ? AND name = 'GOOD NAME'", mmsi);
        assertEquals(0L, jdbc.queryForObject(
                "SELECT count(*) FROM identity_report WHERE mmsi = ? AND name LIKE 'BAD%'", Long.class, mmsi));
    }

    static TrackPoint point(String mmsi, Instant time) {
        return new TrackPoint(mmsi, time, 60.0, 25.0, 11.5, 270.0, 268.0, 0, AisSource.DIGITRAFFIC, CELL);
    }

    private static VesselStatic staticReport(String mmsi, Instant at, String name, String destination, Integer length) {
        return new VesselStatic(mmsi, at, AisSource.AISSTREAM, "9074729", name, "ABCD", 80, 11.2, destination,
                length, length == null ? null : 44, null);
    }

    private static CoverageCell coverage(String cell, int vessels) {
        return new CoverageCell(cell, T0, Map.of(AisSource.AISSTREAM, 40L, AisSource.DIGITRAFFIC, 300L), vessels, 1,
                Map.of(AisSource.AISSTREAM, 900L), 2);
    }
}
