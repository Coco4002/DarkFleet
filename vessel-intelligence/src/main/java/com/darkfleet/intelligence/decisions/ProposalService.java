package com.darkfleet.intelligence.decisions;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.intelligence.audit.AuditLog;
import com.darkfleet.intelligence.decisions.Proposals.CasePayload;
import com.darkfleet.intelligence.decisions.Proposals.ProposalRequest;
import com.darkfleet.intelligence.decisions.Proposals.ProposalView;
import com.darkfleet.intelligence.decisions.Proposals.RuleChangePayload;
import com.darkfleet.intelligence.decisions.Proposals.Status;
import com.darkfleet.intelligence.decisions.Proposals.Type;
import com.darkfleet.intelligence.decisions.Proposals.WatchlistPayload;
import com.darkfleet.intelligence.events.AnalystEvent;
import com.darkfleet.intelligence.web.ApiException;
import com.darkfleet.intelligence.web.Caller;
import com.darkfleet.intelligence.web.PageResult;
import com.darkfleet.intelligence.web.Rows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Proposals and their decisions. Anyone allowed to propose (the agent through the MCP server, or an
 * analyst) creates them; only an analyst approves or rejects, and a rule change needs an administrator.
 * Approval applies the change, records the decision and the audit entry in one transaction: either all
 * of it happens or none. A proposal is decided once; a second decision gets 409.
 */
@Service
public class ProposalService {

    private static final Logger log = LoggerFactory.getLogger(ProposalService.class);
    private static final int MAX_TEXT = 4000;
    private static final int MAX_JSON = 64 * 1024;

    public record Created(ProposalView proposal, boolean created) {
    }

    private static final RowMapper<ProposalView> VIEW = (rs, i) -> {
        BigDecimal confidence = rs.getBigDecimal("confidence");
        return new ProposalView(rs.getString("id"), Type.valueOf(rs.getString("type")),
                Status.valueOf(rs.getString("status")), Rows.json(rs, "payload"), rs.getString("rationale"),
                Rows.json(rs, "evidence"), confidence == null ? null : confidence.doubleValue(),
                rs.getString("proposed_by"), rs.getString("request_id"), Rows.instant(rs, "created_at"),
                rs.getString("decided_by"), Rows.instant(rs, "decided_at"), rs.getString("decision_note"),
                rs.getString("result_type"), rs.getString("result_id"));
    };

    private final NamedParameterJdbcTemplate jdbc;
    private final Cases cases;
    private final Watchlist watchlist;
    private final RuleSets rules;
    private final AuditLog audit;
    private final ApplicationEventPublisher events;
    private final Clock clock;

    public ProposalService(NamedParameterJdbcTemplate jdbc, Cases cases, Watchlist watchlist, RuleSets rules,
                           AuditLog audit, ApplicationEventPublisher events, Clock clock) {
        this.jdbc = jdbc;
        this.cases = cases;
        this.watchlist = watchlist;
        this.rules = rules;
        this.audit = audit;
        this.events = events;
        this.clock = clock;
    }

    @Transactional
    public Created create(ProposalRequest request, Caller caller) {
        check(request);
        var params = new MapSqlParameterSource("id", UUID.randomUUID())
                .addValue("type", request.type().name())
                .addValue("payload", json(request.payload()))
                .addValue("rationale", request.rationale().strip())
                .addValue("evidence", json(request.evidence() == null ? JsonNodeFactory.instance.objectNode() : request.evidence()))
                .addValue("confidence", request.confidence())
                .addValue("by", caller.name())
                .addValue("request", request.requestId());
        List<ProposalView> inserted = jdbc.query("""
                INSERT INTO proposal (id, type, status, payload, rationale, evidence, confidence, proposed_by, request_id)
                VALUES (:id, :type, 'PENDING', :payload::jsonb, :rationale, :evidence::jsonb, :confidence, :by, :request)
                ON CONFLICT ON CONSTRAINT proposal_request DO NOTHING RETURNING *""", params, VIEW);
        if (inserted.isEmpty()) {
            return new Created(jdbc.query("SELECT * FROM proposal WHERE proposed_by = :by AND request_id = :request",
                    params, VIEW).getFirst(), false);
        }
        ProposalView created = inserted.getFirst();
        events.publishEvent(new AnalystEvent("proposal", created));
        log.info("Proposal {} ({}) from {}", created.id(), created.type(), caller.name());
        return new Created(created, true);
    }

    @Transactional
    public ProposalView approve(UUID id, Caller caller, String note) {
        ProposalView before = pendingForDecision(id);
        if (before.type() == Type.RULE_CHANGE && !caller.admin()) {
            throw new AccessDeniedException("Only an administrator can approve a rule change");
        }
        String resultType;
        String resultId;
        switch (before.type()) {
            case CASE -> {
                CasePayload payload = payload(before, CasePayload.class);
                requireValid(cases.problems(payload));
                resultType = "CASE";
                resultId = cases.open(id, payload, caller.name()).toString();
            }
            case WATCHLIST_CHANGE -> {
                WatchlistPayload payload = payload(before, WatchlistPayload.class);
                requireValid(watchlist.problems(payload));
                resultType = "WATCHLIST_ENTRY";
                resultId = watchlist.apply(id, payload, caller.name()).toString();
            }
            case RULE_CHANGE -> {
                RuleChangePayload payload = payload(before, RuleChangePayload.class);
                requireValid(payload.problems());
                resultType = "RULE_SET";
                resultId = String.valueOf(rules.approve(id, payload, caller.name()));
            }
            default -> throw new IllegalStateException(before.type().name());
        }
        ProposalView after = decide(id, Status.APPROVED, caller, note, resultType, resultId);
        audit.record(caller.name(), "APPROVE", "PROPOSAL", id.toString(), before, after);
        events.publishEvent(new AnalystEvent("proposal", after));
        log.info("Proposal {} ({}) approved by {}: {} {}", id, before.type(), caller.name(), resultType, resultId);
        return after;
    }

    @Transactional
    public ProposalView reject(UUID id, Caller caller, String note) {
        if (note == null || note.isBlank()) {
            throw ApiException.badRequest("Rejecting a proposal needs a note saying why");
        }
        ProposalView before = pendingForDecision(id);
        if (before.type() == Type.RULE_CHANGE && !caller.admin()) {
            throw new AccessDeniedException("Only an administrator can decide a rule change");
        }
        ProposalView after = decide(id, Status.REJECTED, caller, note, null, null);
        audit.record(caller.name(), "REJECT", "PROPOSAL", id.toString(), before, after);
        events.publishEvent(new AnalystEvent("proposal", after));
        log.info("Proposal {} ({}) rejected by {}", id, before.type(), caller.name());
        return after;
    }

    @Transactional(readOnly = true)
    public ProposalView get(UUID id) {
        return jdbc.query("SELECT * FROM proposal WHERE id = :id", new MapSqlParameterSource("id", id), VIEW)
                .stream().findFirst().orElseThrow(() -> ApiException.notFound("No proposal " + id));
    }

    @Transactional(readOnly = true)
    public PageResult<ProposalView> list(Status status, Type type, int page, int size) {
        PageResult.check(page, size);
        List<String> where = new ArrayList<>();
        var params = new MapSqlParameterSource();
        if (status != null) {
            where.add("status = :status");
            params.addValue("status", status.name());
        }
        if (type != null) {
            where.add("type = :type");
            params.addValue("type", type.name());
        }
        return PageResult.of(jdbc.query("SELECT * FROM proposal"
                + (where.isEmpty() ? "" : " WHERE " + String.join(" AND ", where))
                + " ORDER BY created_at DESC, id" + PageResult.limit(page, size), params, VIEW), page, size);
    }

    /** Locks the proposal row for the rest of the transaction; 404 or 409 if it can't be decided. */
    private ProposalView pendingForDecision(UUID id) {
        ProposalView proposal = jdbc.query("SELECT * FROM proposal WHERE id = :id FOR UPDATE",
                        new MapSqlParameterSource("id", id), VIEW)
                .stream().findFirst().orElseThrow(() -> ApiException.notFound("No proposal " + id));
        if (proposal.status() != Status.PENDING) {
            throw ApiException.conflict("The proposal was already " + proposal.status().name().toLowerCase()
                    + " by " + proposal.decidedBy());
        }
        return proposal;
    }

    private ProposalView decide(UUID id, Status status, Caller caller, String note, String resultType, String resultId) {
        return jdbc.query("""
                UPDATE proposal SET status = :status, decided_by = :by, decided_at = :at, decision_note = :note,
                                    result_type = :resultType, result_id = :resultId, updated_at = now()
                WHERE id = :id RETURNING *""", new MapSqlParameterSource("id", id).addValue("status", status.name())
                .addValue("by", caller.name()).addValue("at", Timestamp.from(clock.instant()))
                .addValue("note", note == null || note.isBlank() ? null : note.strip())
                .addValue("resultType", resultType).addValue("resultId", resultId), VIEW).getFirst();
    }

    /** Shape and content checks before anything is stored: 400 for a malformed request, 422 for bad content. */
    private void check(ProposalRequest r) {
        if (r.type() == null || r.payload() == null || !r.payload().isObject()) {
            throw ApiException.badRequest("type and a payload object are required");
        }
        if (r.rationale() == null || r.rationale().isBlank() || r.rationale().length() > MAX_TEXT) {
            throw ApiException.badRequest("rationale is required, at most " + MAX_TEXT + " characters");
        }
        if (r.requestId() == null || r.requestId().isBlank() || r.requestId().length() > 200) {
            throw ApiException.badRequest("requestId is required, at most 200 characters");
        }
        if (r.evidence() != null && !r.evidence().isObject()) {
            throw ApiException.badRequest("evidence must be a JSON object");
        }
        if (r.confidence() != null && (r.confidence() < 0 || r.confidence() > 1 || r.confidence().isNaN())) {
            throw ApiException.badRequest("confidence must be between 0 and 1");
        }
        if (json(r.payload()).length() > MAX_JSON || r.evidence() != null && json(r.evidence()).length() > MAX_JSON) {
            throw ApiException.badRequest("payload and evidence are at most 64 KB each");
        }
        requireValid(switch (r.type()) {
            case CASE -> cases.problems(convert(r.payload(), CasePayload.class));
            case WATCHLIST_CHANGE -> watchlist.problems(convert(r.payload(), WatchlistPayload.class));
            case RULE_CHANGE -> convert(r.payload(), RuleChangePayload.class).problems();
        });
    }

    private static <T> T payload(ProposalView proposal, Class<T> type) {
        return convert(proposal.payload(), type);
    }

    private static <T> T convert(JsonNode payload, Class<T> type) {
        try {
            return JsonSerde.mapper().treeToValue(payload, type);
        } catch (JacksonException e) {
            throw ApiException.unprocessable("The payload doesn't fit a " + type.getSimpleName() + ": "
                    + e.getOriginalMessage());
        }
    }

    private static void requireValid(List<String> problems) {
        if (!problems.isEmpty()) {
            throw ApiException.unprocessable(String.join("; ", problems));
        }
    }

    private static String json(JsonNode node) {
        return JsonSerde.mapper().writeValueAsString(node);
    }
}
