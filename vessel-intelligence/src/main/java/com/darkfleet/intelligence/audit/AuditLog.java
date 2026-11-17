package com.darkfleet.intelligence.audit;

import com.darkfleet.common.kafka.JsonSerde;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Records who changed what. Written in the transaction of the change itself, so the log can never
 * claim a change that was rolled back, nor miss one that was committed.
 */
@Repository
public class AuditLog {

    private final JdbcTemplate jdbc;

    public AuditLog(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @param before the state before the change (any JSON-serializable value), null when created
     * @param after  the state after, null when removed
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void record(String actor, String action, String entityType, String entityId, Object before, Object after) {
        jdbc.update("""
                INSERT INTO audit_log (actor, action, entity_type, entity_id, before, after)
                VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb)""",
                actor, action, entityType, entityId, json(before), json(after));
    }

    private static String json(Object value) {
        return value == null ? null : JsonSerde.mapper().writeValueAsString(value);
    }
}
