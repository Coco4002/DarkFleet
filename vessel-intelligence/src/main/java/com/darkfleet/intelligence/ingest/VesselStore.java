package com.darkfleet.intelligence.ingest;

import com.darkfleet.common.model.VesselStatic;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Stores {@code vessel.static}: the latest data per vessel, and the history of identities it
 * broadcast. Both writes are safe to repeat.
 */
@Repository
public class VesselStore {

    /**
     * Only a newer report changes the row; a field the newer report lacks keeps its last known value
     * (feeds and AIS message parts don't all carry every field).
     */
    private static final String UPSERT_VESSEL = """
            INSERT INTO vessel (mmsi, imo, name, call_sign, ship_type, length_m, beam_m, draught_m,
                                destination, eta, source, reported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (mmsi) DO UPDATE SET
                imo         = coalesce(excluded.imo, vessel.imo),
                name        = coalesce(excluded.name, vessel.name),
                call_sign   = coalesce(excluded.call_sign, vessel.call_sign),
                ship_type   = coalesce(excluded.ship_type, vessel.ship_type),
                length_m    = coalesce(excluded.length_m, vessel.length_m),
                beam_m      = coalesce(excluded.beam_m, vessel.beam_m),
                draught_m   = coalesce(excluded.draught_m, vessel.draught_m),
                destination = coalesce(excluded.destination, vessel.destination),
                eta         = coalesce(excluded.eta, vessel.eta),
                source      = excluded.source,
                reported_at = excluded.reported_at,
                updated_at  = now()
            WHERE vessel.reported_at < excluded.reported_at
            """;

    /** The same identity seen again only widens the period it was seen in. */
    private static final String UPSERT_IDENTITY = """
            INSERT INTO identity_report (mmsi, source, imo, name, call_sign, ship_type, length_m, beam_m,
                                         first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT ON CONSTRAINT identity_report_identity DO UPDATE SET
                ship_type  = CASE WHEN excluded.last_seen >= identity_report.last_seen
                                  THEN coalesce(excluded.ship_type, identity_report.ship_type)
                                  ELSE identity_report.ship_type END,
                length_m   = CASE WHEN excluded.last_seen >= identity_report.last_seen
                                  THEN coalesce(excluded.length_m, identity_report.length_m)
                                  ELSE identity_report.length_m END,
                beam_m     = CASE WHEN excluded.last_seen >= identity_report.last_seen
                                  THEN coalesce(excluded.beam_m, identity_report.beam_m)
                                  ELSE identity_report.beam_m END,
                first_seen = least(identity_report.first_seen, excluded.first_seen),
                last_seen  = greatest(identity_report.last_seen, excluded.last_seen),
                updated_at = now()
            """;

    private final JdbcTemplate jdbc;

    public VesselStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public void save(List<VesselStatic> reports) {
        if (reports.isEmpty()) {
            return;
        }
        jdbc.batchUpdate(UPSERT_VESSEL, reports.stream().map(v -> new Object[] {
                v.mmsi(), v.imoNumber(), v.vesselName(), v.callSign(), v.shipType(), v.lengthMeters(),
                v.beamMeters(), v.draughtMeters(), v.destination(), Sql.ts(v.eta()), Sql.name(v.source()),
                Sql.ts(v.reportedAt())
        }).toList());

        List<Object[]> identities = reports.stream()
                .filter(v -> v.imoNumber() != null || v.vesselName() != null || v.callSign() != null)
                .map(v -> new Object[] {
                        v.mmsi(), Sql.name(v.source()), v.imoNumber(), v.vesselName(), v.callSign(), v.shipType(),
                        v.lengthMeters(), v.beamMeters(), Sql.ts(v.reportedAt()), Sql.ts(v.reportedAt())
                }).toList();
        if (!identities.isEmpty()) {
            jdbc.batchUpdate(UPSERT_IDENTITY, identities);
        }
    }
}
