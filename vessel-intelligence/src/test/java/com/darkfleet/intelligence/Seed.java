package com.darkfleet.intelligence;

import com.darkfleet.intelligence.ingest.TrackPartitions;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/** Writes test data straight into the tables, the way the consumers would. Every insert is repeatable. */
final class Seed {

    private final JdbcTemplate jdbc;
    private final TrackPartitions partitions;

    Seed(JdbcTemplate jdbc, TrackPartitions partitions) {
        this.jdbc = jdbc;
        this.partitions = partitions;
    }

    void vessel(String mmsi, String imo, String name) {
        jdbc.update("""
                INSERT INTO vessel (mmsi, imo, name, call_sign, ship_type, length_m, beam_m, source, reported_at)
                VALUES (?, ?, ?, 'TEST', 80, 183, 32, 'AISSTREAM', '2026-11-12T09:00:00Z') ON CONFLICT DO NOTHING""",
                mmsi, imo, name);
    }

    void identity(String mmsi, String imo, String name, String firstSeen, String lastSeen) {
        jdbc.update("""
                INSERT INTO identity_report (mmsi, source, imo, name, call_sign, first_seen, last_seen)
                VALUES (?, 'AISSTREAM', ?, ?, 'TEST', ?::timestamptz, ?::timestamptz) ON CONFLICT DO NOTHING""",
                mmsi, imo, name, firstSeen, lastSeen);
    }

    void detection(String id, String type, String status, String mmsi, String otherMmsi, String startedAt,
                   String endedAt, Double lat, Double lon, String cell) {
        jdbc.update("""
                INSERT INTO detection (detection_id, type, status, version, mmsi, other_mmsi, started_at, ended_at,
                                       position, h3_cell, rules_version, evidence)
                VALUES (?, ?, ?, 1, ?, ?, ?::timestamptz, ?::timestamptz,
                        ST_SetSRID(ST_MakePoint(?::float8, ?::float8), 4326), ?, 0, '{"note":"seeded"}')
                ON CONFLICT DO NOTHING""", id, type, status, mmsi, otherMmsi, startedAt, endedAt, lon, lat, cell);
    }

    void trackPoints(String mmsi, String cell, Instant... times) {
        partitions.ensure(List.of(times));
        for (Instant t : times) {
            jdbc.update("""
                    INSERT INTO track_point (mmsi, time, position, sog_knots, cog_degrees, source, h3_cell)
                    VALUES (?, ?, ST_SetSRID(ST_MakePoint(24.0, 59.5), 4326), 0.4, 90, 'DIGITRAFFIC', ?)
                    ON CONFLICT DO NOTHING""", mmsi, Timestamp.from(t), cell);
        }
    }

    void keptTrackPoint(String mmsi, Instant time) {
        jdbc.update("""
                INSERT INTO track_point_kept (mmsi, time, position, sog_knots, source, h3_cell)
                VALUES (?, ?, ST_SetSRID(ST_MakePoint(24.0, 59.5), 4326), 0.2, 'AISSTREAM', 'kept')
                ON CONFLICT DO NOTHING""", mmsi, Timestamp.from(time));
    }

    void coverage(String cell, String hour) {
        jdbc.update("""
                INSERT INTO coverage_cell (h3_cell, hour_start, positions_by_source, vessels, stopped_vessels,
                                           window_positions_by_source, window_max_stopped_vessels)
                VALUES (?, ?::timestamptz, '{"AISSTREAM": 12, "DIGITRAFFIC": 340}', 4, 1, '{"AISSTREAM": 300}', 2)
                ON CONFLICT DO NOTHING""", cell, hour);
    }
}
