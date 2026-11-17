package com.darkfleet.intelligence.decisions;

import com.darkfleet.intelligence.audit.AuditLog;
import com.darkfleet.intelligence.read.Mappers;
import com.darkfleet.intelligence.read.Views.CaseSummary;
import com.darkfleet.intelligence.read.Views.DetectionView;
import com.darkfleet.intelligence.web.ApiException;
import com.darkfleet.intelligence.web.PageResult;
import com.darkfleet.intelligence.web.Rows;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Cases: opened only by approving a proposal, closed by an analyst. Opening one copies its evidence
 * (the detections, both vessels' tracks from 6 h before to 6 h after, coverage of the area, identity
 * history), so the case stays complete after track history expires.
 */
@Component
public class Cases {

    public record CaseDetail(CaseSummary summary, String proposalId, String openedBy, String closedBy,
                             String closeNote, List<DetectionView> detections, List<EvidenceItem> evidence) {
    }

    public record EvidenceItem(String kind, String ref, JsonNode data) {
    }

    static final int MAX_DETECTIONS = 50;
    private static final Pattern MMSI = Pattern.compile("\\d{9}");

    /** Track snippet per detection and vessel: 6 h either side, at most 7 days after the start. */
    private static final String COPY_TRACKS = """
            INSERT INTO vessel_case_evidence (case_id, kind, ref, data)
            SELECT :case, 'TRACK', d.detection_id || '/' || m.mmsi, coalesce((
                SELECT jsonb_agg(jsonb_build_object(
                           'mmsi', t.mmsi, 'time', t.time, 'latitude', ST_Y(t.position), 'longitude', ST_X(t.position),
                           'sogKnots', t.sog_knots, 'cogDegrees', t.cog_degrees, 'source', t.source) ORDER BY t.time)
                FROM (SELECT * FROM track_point UNION SELECT * FROM track_point_kept) t
                WHERE t.mmsi = m.mmsi
                  AND t.time >= d.started_at - interval '6 hours'
                  AND t.time <= least(coalesce(d.ended_at, now()) + interval '6 hours', d.started_at + interval '7 days')
            ), '[]'::jsonb)
            FROM detection d CROSS JOIN LATERAL (VALUES (d.mmsi), (d.other_mmsi)) AS m (mmsi)
            WHERE d.detection_id IN (:ids) AND m.mmsi IS NOT NULL""";

    private static final String COPY_DETECTIONS = """
            INSERT INTO vessel_case_evidence (case_id, kind, ref, data)
            SELECT :case, 'DETECTION', d.detection_id, jsonb_build_object(
                'detectionId', d.detection_id, 'type', d.type, 'status', d.status, 'version', d.version,
                'mmsi', d.mmsi, 'otherMmsi', d.other_mmsi, 'startedAt', d.started_at, 'endedAt', d.ended_at,
                'latitude', ST_Y(d.position), 'longitude', ST_X(d.position), 'h3Cell', d.h3_cell,
                'rulesVersion', d.rules_version, 'evidence', d.evidence)
            FROM detection d WHERE d.detection_id IN (:ids)""";

    /** Coverage of the detection's cell from 6 h before it started until it ended. */
    private static final String COPY_COVERAGE = """
            INSERT INTO vessel_case_evidence (case_id, kind, ref, data)
            SELECT :case, 'COVERAGE', d.detection_id, coalesce((
                SELECT jsonb_agg(jsonb_build_object(
                           'h3Cell', c.h3_cell, 'hourStart', c.hour_start, 'positionsBySource', c.positions_by_source,
                           'vessels', c.vessels, 'stoppedVessels', c.stopped_vessels,
                           'windowPositionsBySource', c.window_positions_by_source) ORDER BY c.hour_start)
                FROM coverage_cell c
                WHERE c.h3_cell = d.h3_cell
                  AND c.hour_start >= date_trunc('hour', d.started_at) - interval '6 hours'
                  AND c.hour_start <= least(coalesce(d.ended_at, now()), d.started_at + interval '7 days')
            ), '[]'::jsonb)
            FROM detection d WHERE d.detection_id IN (:ids) AND d.h3_cell IS NOT NULL""";

    private static final String COPY_IDENTITIES = """
            INSERT INTO vessel_case_evidence (case_id, kind, ref, data)
            SELECT :case, 'IDENTITY', v.mmsi, coalesce((
                SELECT jsonb_agg(jsonb_build_object(
                           'source', r.source, 'imo', r.imo, 'name', r.name, 'callSign', r.call_sign,
                           'shipType', r.ship_type, 'lengthMeters', r.length_m, 'beamMeters', r.beam_m,
                           'firstSeen', r.first_seen, 'lastSeen', r.last_seen) ORDER BY r.first_seen)
                FROM identity_report r WHERE r.mmsi = v.mmsi
            ), '[]'::jsonb)
            FROM (SELECT mmsi FROM detection WHERE detection_id IN (:ids)
                  UNION SELECT other_mmsi FROM detection WHERE detection_id IN (:ids) AND other_mmsi IS NOT NULL) v""";

    private final NamedParameterJdbcTemplate jdbc;
    private final AuditLog audit;
    private final Clock clock;

    public Cases(NamedParameterJdbcTemplate jdbc, AuditLog audit, Clock clock) {
        this.jdbc = jdbc;
        this.audit = audit;
        this.clock = clock;
    }

    /** @return what is wrong with the payload, empty when it can be approved */
    @Transactional(readOnly = true)
    public List<String> problems(Proposals.CasePayload p) {
        List<String> problems = new ArrayList<>();
        if (p.title() == null || p.title().isBlank() || p.title().length() > 200) {
            problems.add("title is required, at most 200 characters");
        }
        if (p.mmsi() == null || !MMSI.matcher(p.mmsi()).matches()) {
            problems.add("mmsi must have 9 digits");
        }
        if (p.detectionIds() == null || p.detectionIds().isEmpty() || p.detectionIds().size() > MAX_DETECTIONS) {
            problems.add("detectionIds must list 1 to " + MAX_DETECTIONS + " detections");
        } else if (problems.isEmpty()) {
            List<String> ids = p.detectionIds().stream().distinct().toList();
            List<String> matching = jdbc.queryForList("""
                    SELECT detection_id FROM detection
                    WHERE detection_id IN (:ids) AND (mmsi = :mmsi OR other_mmsi = :mmsi)""",
                    new MapSqlParameterSource("ids", ids).addValue("mmsi", p.mmsi()), String.class);
            ids.stream().filter(id -> !matching.contains(id))
                    .forEach(id -> problems.add("detection " + id + " doesn't exist or isn't about vessel " + p.mmsi()));
        }
        return problems;
    }

    /** Opens the case an approved proposal asks for. Runs inside the approval's transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    UUID open(UUID proposalId, Proposals.CasePayload p, String openedBy) {
        UUID id = UUID.randomUUID();
        List<String> ids = p.detectionIds().stream().distinct().toList();
        var params = new MapSqlParameterSource("case", id).addValue("ids", ids);
        jdbc.update("""
                INSERT INTO vessel_case (id, title, mmsi, status, proposal_id, opened_by, opened_at)
                VALUES (:case, :title, :mmsi, 'OPEN', :proposal, :by, :at)""",
                new MapSqlParameterSource(params.getValues()).addValue("title", p.title().strip())
                        .addValue("mmsi", p.mmsi()).addValue("proposal", proposalId).addValue("by", openedBy)
                        .addValue("at", Timestamp.from(clock.instant())));
        jdbc.update("""
                INSERT INTO vessel_case_detection (case_id, detection_id)
                SELECT :case, detection_id FROM detection WHERE detection_id IN (:ids)""", params);
        jdbc.update(COPY_DETECTIONS, params);
        jdbc.update(COPY_TRACKS, params);
        jdbc.update(COPY_COVERAGE, params);
        jdbc.update(COPY_IDENTITIES, params);
        return id;
    }

    @Transactional
    public CaseDetail close(UUID id, String closedBy, String note) {
        if (note == null || note.isBlank()) {
            throw ApiException.badRequest("Closing a case needs a note saying why");
        }
        CaseDetail before = get(id);
        if (!"OPEN".equals(before.summary().status())) {
            throw ApiException.conflict("The case is already closed");
        }
        // only an open case is closed: of two analysts closing it at once, the second gets 409
        int closed = jdbc.update("""
                UPDATE vessel_case SET status = 'CLOSED', closed_by = :by, closed_at = :at, close_note = :note,
                                       updated_at = now()
                WHERE id = :id AND status = 'OPEN'""", new MapSqlParameterSource("id", id).addValue("by", closedBy)
                .addValue("at", Timestamp.from(clock.instant())).addValue("note", note.strip()));
        if (closed == 0) {
            throw ApiException.conflict("The case is already closed");
        }
        CaseDetail after = get(id);
        audit.record(closedBy, "CLOSE", "CASE", id.toString(), before.summary(), after.summary());
        return after;
    }

    @Transactional(readOnly = true)
    public PageResult<CaseSummary> list(String status, String mmsi, int page, int size) {
        PageResult.check(page, size);
        List<String> where = new ArrayList<>();
        var params = new MapSqlParameterSource();
        if (status != null) {
            if (!status.equals("OPEN") && !status.equals("CLOSED")) {
                throw ApiException.badRequest("status is OPEN or CLOSED");
            }
            where.add("status = :status");
            params.addValue("status", status);
        }
        if (mmsi != null) {
            where.add("mmsi = :mmsi");
            params.addValue("mmsi", mmsi);
        }
        return PageResult.of(jdbc.query("SELECT * FROM vessel_case"
                + (where.isEmpty() ? "" : " WHERE " + String.join(" AND ", where))
                + " ORDER BY opened_at DESC, id" + PageResult.limit(page, size), params, Mappers.CASE_SUMMARY), page, size);
    }

    @Transactional(readOnly = true)
    public CaseDetail get(UUID id) {
        var params = new MapSqlParameterSource("id", id);
        record Row(CaseSummary summary, String proposalId, String openedBy, String closedBy, String closeNote) {
        }
        Row row = jdbc.query("SELECT * FROM vessel_case WHERE id = :id", params, (rs, i) -> new Row(
                        Mappers.CASE_SUMMARY.mapRow(rs, i), rs.getString("proposal_id"), rs.getString("opened_by"),
                        rs.getString("closed_by"), rs.getString("close_note")))
                .stream().findFirst().orElseThrow(() -> ApiException.notFound("No case " + id));
        List<DetectionView> detections = jdbc.query("SELECT " + Mappers.DETECTION_COLUMNS + """
                 FROM detection d JOIN vessel_case_detection cd ON cd.detection_id = d.detection_id
                WHERE cd.case_id = :id ORDER BY d.started_at""", params, Mappers.DETECTION);
        List<EvidenceItem> evidence = jdbc.query("""
                SELECT kind, ref, data FROM vessel_case_evidence WHERE case_id = :id ORDER BY id""", params,
                (rs, i) -> new EvidenceItem(rs.getString("kind"), rs.getString("ref"), Rows.json(rs, "data")));
        return new CaseDetail(row.summary(), row.proposalId(), row.openedBy(), row.closedBy(), row.closeNote(),
                detections, evidence);
    }
}
