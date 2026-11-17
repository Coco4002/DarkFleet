package com.darkfleet.intelligence.decisions;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Events waiting to be published to Kafka. Added in the transaction of the change they announce:
 * if that transaction rolls back, the event never existed; if it commits, {@link OutboxRelay} publishes
 * it, however long Kafka is down.
 */
@Repository
public class Outbox {

    private final JdbcTemplate jdbc;

    public Outbox(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @param json the message, already serialized as the consumers expect it */
    @Transactional(propagation = Propagation.MANDATORY)
    public void add(String topic, String key, String json) {
        jdbc.update("INSERT INTO outbox (topic, key, payload) VALUES (?, ?, ?::jsonb)", topic, key, json);
    }
}
