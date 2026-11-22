package com.darkfleet.intelligence.read;

import com.darkfleet.intelligence.read.Views.CoverageView;
import com.darkfleet.intelligence.web.Rows;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Repository
@Transactional(readOnly = true)
public class CoverageQueries {

    private final JdbcTemplate jdbc;

    public CoverageQueries(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String SELECT = """
            SELECT h3_cell, hour_start, positions_by_source, vessels, stopped_vessels,
                   window_positions_by_source, window_max_stopped_vessels
            FROM coverage_cell""";

    private static final RowMapper<CoverageView> CELL = (rs, i) -> new CoverageView(
            rs.getString("h3_cell"), Rows.instant(rs, "hour_start"), Rows.json(rs, "positions_by_source"),
            rs.getInt("vessels"), rs.getInt("stopped_vessels"), Rows.json(rs, "window_positions_by_source"),
            rs.getInt("window_max_stopped_vessels"));

    /** Every cell's statistics for the hour containing {@code time}. */
    public List<CoverageView> hour(Instant time) {
        Instant hour = time.truncatedTo(ChronoUnit.HOURS);
        return jdbc.query(SELECT + " WHERE hour_start = ? ORDER BY h3_cell", CELL, Timestamp.from(hour));
    }

    /** The newest published hour: an hour is published only after it ends (plus a grace period). */
    public List<CoverageView> latest() {
        return jdbc.query(SELECT + " WHERE hour_start = (SELECT max(hour_start) FROM coverage_cell) ORDER BY h3_cell",
                CELL);
    }
}
