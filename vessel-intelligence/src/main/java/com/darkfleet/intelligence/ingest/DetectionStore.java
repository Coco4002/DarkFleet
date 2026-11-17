package com.darkfleet.intelligence.ingest;

import com.darkfleet.common.model.Detection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores {@code detections}, one row per detection holding its newest version. An update with a version
 * that isn't newer than the stored one (a repeat, or one delivered late) changes nothing.
 */
@Repository
public class DetectionStore {

    private static final String UPSERT = """
            INSERT INTO detection (detection_id, type, status, version, mmsi, other_mmsi, started_at, ended_at,
                                   position, h3_cell, rules_version, evidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?,
                    public.ST_SetSRID(public.ST_MakePoint(?::float8, ?::float8), 4326), ?, ?, ?::jsonb)
            ON CONFLICT (detection_id) DO UPDATE SET
                type          = excluded.type,
                status        = excluded.status,
                version       = excluded.version,
                mmsi          = excluded.mmsi,
                other_mmsi    = excluded.other_mmsi,
                started_at    = excluded.started_at,
                ended_at      = excluded.ended_at,
                position      = excluded.position,
                h3_cell       = excluded.h3_cell,
                rules_version = excluded.rules_version,
                evidence      = excluded.evidence,
                updated_at    = now()
            WHERE detection.version < excluded.version
            """;

    private final JdbcTemplate jdbc;

    public DetectionStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return the detections that were new or newer than what was stored, in the order given */
    @Transactional
    public List<Detection> save(List<Detection> detections) {
        if (detections.isEmpty()) {
            return List.of();
        }
        int[] counts = jdbc.batchUpdate(UPSERT, detections.stream().map(d -> new Object[] {
                d.detectionId(), Sql.name(d.type()), Sql.name(d.status()), d.version(), d.mmsi(), d.otherMmsi(),
                Sql.ts(d.startedAt()), Sql.ts(d.endedAt()), d.longitude(), d.latitude(), d.h3Cell(),
                d.rulesVersion(), Sql.json(d.evidence())
        }).toList());
        List<Detection> changed = new ArrayList<>();
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] > 0) {
                changed.add(detections.get(i));
            }
        }
        return changed;
    }
}
