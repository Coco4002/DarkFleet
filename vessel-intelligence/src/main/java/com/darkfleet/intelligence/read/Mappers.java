package com.darkfleet.intelligence.read;

import com.darkfleet.intelligence.read.Views.AssessmentView;
import com.darkfleet.intelligence.read.Views.CaseSummary;
import com.darkfleet.intelligence.read.Views.DetectionView;
import com.darkfleet.intelligence.read.Views.TrackPointView;
import com.darkfleet.intelligence.read.Views.WatchlistView;
import com.darkfleet.intelligence.web.Rows;
import org.springframework.jdbc.core.RowMapper;

/** Row mappers shared by the queries; each expects the columns listed next to it. */
public final class Mappers {

    private Mappers() {
    }

    /** Columns for {@link #DETECTION}. */
    public static final String DETECTION_COLUMNS = """
            d.detection_id, d.type, d.status, d.version, d.mmsi, d.other_mmsi, d.started_at, d.ended_at,
            ST_Y(d.position) AS lat, ST_X(d.position) AS lon, d.h3_cell, d.rules_version, d.evidence, d.updated_at""";

    public static final RowMapper<DetectionView> DETECTION = (rs, i) -> new DetectionView(
            rs.getString("detection_id"), rs.getString("type"), rs.getString("status"), rs.getInt("version"),
            rs.getString("mmsi"), rs.getString("other_mmsi"), Rows.instant(rs, "started_at"),
            Rows.instant(rs, "ended_at"), Rows.decimal(rs, "lat"), Rows.decimal(rs, "lon"), rs.getString("h3_cell"),
            rs.getInt("rules_version"), Rows.json(rs, "evidence"), Rows.instant(rs, "updated_at"));

    /** Columns for {@link #TRACK_POINT}. */
    public static final String TRACK_POINT_COLUMNS = """
            time, ST_Y(position) AS lat, ST_X(position) AS lon, sog_knots, cog_degrees, heading_degrees,
            nav_status, source""";

    public static final RowMapper<TrackPointView> TRACK_POINT = (rs, i) -> new TrackPointView(
            Rows.instant(rs, "time"), rs.getDouble("lat"), rs.getDouble("lon"), Rows.decimal(rs, "sog_knots"),
            Rows.decimal(rs, "cog_degrees"), Rows.decimal(rs, "heading_degrees"), Rows.integer(rs, "nav_status"),
            rs.getString("source"));

    public static final RowMapper<WatchlistView> WATCHLIST = (rs, i) -> new WatchlistView(
            rs.getString("id"), rs.getString("mmsi"), rs.getString("imo"), rs.getString("reason"),
            rs.getBoolean("active"), rs.getString("proposal_id"), rs.getString("added_by"),
            Rows.instant(rs, "added_at"), rs.getString("removed_by"), Rows.instant(rs, "removed_at"));

    public static final RowMapper<CaseSummary> CASE_SUMMARY = (rs, i) -> new CaseSummary(
            rs.getString("id"), rs.getString("title"), rs.getString("mmsi"), rs.getString("status"),
            Rows.instant(rs, "opened_at"), Rows.instant(rs, "closed_at"));

    public static final RowMapper<AssessmentView> ASSESSMENT = (rs, i) -> new AssessmentView(
            rs.getString("id"), rs.getString("detection_id"), rs.getString("category"), rs.getString("rationale"),
            rs.getString("assessed_by"), Rows.instant(rs, "created_at"));
}
