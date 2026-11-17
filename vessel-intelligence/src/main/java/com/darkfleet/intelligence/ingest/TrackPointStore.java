package com.darkfleet.intelligence.ingest;

import com.darkfleet.common.model.TrackPoint;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Stores {@code track.points}. A point already stored for that vessel and time is left as it is. */
@Repository
public class TrackPointStore {

    private static final String INSERT = """
            INSERT INTO track_point (mmsi, time, position, sog_knots, cog_degrees, heading_degrees,
                                     nav_status, source, h3_cell)
            VALUES (?, ?, public.ST_SetSRID(public.ST_MakePoint(?, ?), 4326), ?, ?, ?, ?, ?, ?)
            ON CONFLICT (mmsi, time) DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public TrackPointStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The partitions for the points' days must exist ({@link TrackPartitions#ensure}). */
    @Transactional
    public void save(List<TrackPoint> points) {
        if (points.isEmpty()) {
            return;
        }
        jdbc.batchUpdate(INSERT, points.stream().map(p -> new Object[] {
                p.mmsi(), Sql.ts(p.time()), p.longitude(), p.latitude(), p.sogKnots(), p.cogDegrees(),
                p.headingDegrees(), p.navStatus(), Sql.name(p.source()), p.h3Cell()
        }).toList());
    }

    /** False for a position no real vessel can report; the database would accept it. */
    public static boolean valid(TrackPoint point) {
        return Double.isFinite(point.latitude()) && Double.isFinite(point.longitude())
                && Math.abs(point.latitude()) <= 90 && Math.abs(point.longitude()) <= 180;
    }
}
