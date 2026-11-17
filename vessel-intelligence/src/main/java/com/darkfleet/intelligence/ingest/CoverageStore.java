package com.darkfleet.intelligence.ingest;

import com.darkfleet.common.model.CoverageCell;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Stores {@code coverage.cells}; a later record for the same cell and hour replaces the earlier one. */
@Repository
public class CoverageStore {

    private static final String UPSERT = """
            INSERT INTO coverage_cell (h3_cell, hour_start, positions_by_source, vessels, stopped_vessels,
                                       window_positions_by_source, window_max_stopped_vessels)
            VALUES (?, ?, ?::jsonb, ?, ?, ?::jsonb, ?)
            ON CONFLICT (h3_cell, hour_start) DO UPDATE SET
                positions_by_source        = excluded.positions_by_source,
                vessels                    = excluded.vessels,
                stopped_vessels            = excluded.stopped_vessels,
                window_positions_by_source = excluded.window_positions_by_source,
                window_max_stopped_vessels = excluded.window_max_stopped_vessels,
                updated_at                 = now()
            """;

    private final JdbcTemplate jdbc;

    public CoverageStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public void save(List<CoverageCell> cells) {
        if (cells.isEmpty()) {
            return;
        }
        jdbc.batchUpdate(UPSERT, cells.stream().map(c -> new Object[] {
                c.h3Cell(), Sql.ts(c.hourStart()), Sql.json(c.positionsBySource()), c.vessels(), c.stoppedVessels(),
                Sql.json(c.windowPositionsBySource()), c.windowMaxStoppedVessels()
        }).toList());
    }
}
