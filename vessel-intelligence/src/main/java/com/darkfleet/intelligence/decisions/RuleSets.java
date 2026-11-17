package com.darkfleet.intelligence.decisions;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.intelligence.IntelligenceTopics;
import com.darkfleet.intelligence.audit.AuditLog;
import com.darkfleet.intelligence.web.PageResult;
import com.darkfleet.intelligence.web.Rows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * The detection rule sets. Version 0 is the shared baseline, stored at start-up; every approved rule
 * change becomes the next version and is announced on {@code rules.approved} through the outbox, in the
 * same transaction, so the database and Track &amp; Detection can't disagree for good.
 */
@Component
public class RuleSets implements ApplicationRunner {

    public record RuleSetView(int version, DetectionRules rules, String proposalId, String approvedBy,
                              Instant approvedAt) {
    }

    private static final RowMapper<RuleSetView> VIEW = (rs, i) -> new RuleSetView(
            rs.getInt("version"), JsonSerde.mapper().readValue(rs.getString("rules"), DetectionRules.class),
            rs.getString("proposal_id"), rs.getString("approved_by"), Rows.instant(rs, "approved_at"));

    private static final Logger log = LoggerFactory.getLogger(RuleSets.class);

    private final JdbcTemplate jdbc;
    private final Outbox outbox;
    private final IntelligenceTopics topics;
    private final AuditLog audit;
    private final Clock clock;

    public RuleSets(JdbcTemplate jdbc, Outbox outbox, IntelligenceTopics topics, AuditLog audit, Clock clock) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.topics = topics;
        this.audit = audit;
        this.clock = clock;
    }

    /**
     * Stores the shared baseline as version 0, or updates version 0 when the baseline file changed since
     * (thresholds tuned in a new release), so this service shows what Track &amp; Detection applies.
     */
    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        DetectionRules baseline = DetectionRules.baseline();
        String json = JsonSerde.mapper().writeValueAsString(baseline);
        DetectionRules stored = jdbc.query("SELECT rules::text FROM rule_set WHERE version = 0 FOR UPDATE",
                        (rs, i) -> JsonSerde.mapper().readValue(rs.getString(1), DetectionRules.class))
                .stream().findFirst().orElse(null);
        if (stored == null) {
            jdbc.update("INSERT INTO rule_set (version, rules, approved_by, approved_at) VALUES (0, ?::jsonb, 'baseline', ?)",
                    json, Timestamp.from(clock.instant()));
        } else if (!stored.equals(baseline)) {
            jdbc.update("UPDATE rule_set SET rules = ?::jsonb, approved_at = ? WHERE version = 0",
                    json, Timestamp.from(clock.instant()));
            audit.record("baseline", "UPDATE", "RULE_SET", "0", stored, baseline);
            log.info("The baseline rules (version 0) changed: {}", baseline);
        }
    }

    @Transactional(readOnly = true)
    public RuleSetView current() {
        return jdbc.query("SELECT * FROM rule_set ORDER BY version DESC LIMIT 1", VIEW).getFirst();
    }

    @Transactional(readOnly = true)
    public PageResult<RuleSetView> history(int page, int size) {
        PageResult.check(page, size);
        return PageResult.of(jdbc.query("SELECT * FROM rule_set ORDER BY version DESC" + PageResult.limit(page, size),
                VIEW), page, size);
    }

    /**
     * Stores an approved rule change as the next version and queues it for Track &amp; Detection. Approvals
     * are serialized by a lock, so versions and outbox order follow commit order.
     *
     * @return the new version
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public int approve(UUID proposalId, Proposals.RuleChangePayload change, String approvedBy) {
        jdbc.query("SELECT pg_advisory_xact_lock(hashtext('rule_set ' || current_schema()))", rs -> { });
        int version = jdbc.queryForObject("SELECT max(version) + 1 FROM rule_set", Integer.class);
        DetectionRules rules = change.toRules(version);
        String json = JsonSerde.mapper().writeValueAsString(rules);
        jdbc.update("""
                INSERT INTO rule_set (version, rules, proposal_id, approved_by, approved_at)
                VALUES (?, ?::jsonb, ?, ?, ?)""", version, json, proposalId, approvedBy, Timestamp.from(clock.instant()));
        outbox.add(topics.rulesApproved(), DetectionRules.KEY, json);
        return version;
    }

    List<RuleSetView> all() {
        return jdbc.query("SELECT * FROM rule_set ORDER BY version", VIEW);
    }
}
