package com.darkfleet.intelligence.decisions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes the outbox to Kafka every second, oldest first. Each message is sent, acknowledged and
 * marked as published in its own transaction before the next one is read, so a message is never
 * skipped or reordered. A crash between the send and the mark resends only that message, which is
 * then the newest one sent: the last record on {@code rules.approved} is always the latest rule set.
 * This ordering assumes one Vessel Intelligence instance, as deployed: the advisory lock keeps two
 * relays from publishing at the same time, but a send that timed out here could still reach Kafka
 * after another instance's later message.
 */
@Component
class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration WARN_INTERVAL = Duration.ofMinutes(1);

    private record Pending(long id, String topic, String key, String payload) {
    }

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final KafkaTemplate<String, String> kafka;
    private final Clock clock;
    private Instant lastWarning = Instant.EPOCH;

    OutboxRelay(JdbcTemplate jdbc, TransactionTemplate transaction, KafkaTemplate<String, String> kafka, Clock clock) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.kafka = kafka;
        this.clock = clock;
    }

    @Scheduled(fixedDelay = 1000)
    void publish() {
        for (int i = 0; i < BATCH; i++) {
            if (!Boolean.TRUE.equals(transaction.execute(status -> publishOldest()))) {
                return;
            }
        }
    }

    /** @return true when a message was published and the next one may follow */
    private boolean publishOldest() {
        // one relay at a time (see the class comment for what this doesn't cover)
        Boolean mine = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(hashtext('outbox relay ' || current_schema()))", Boolean.class);
        if (!Boolean.TRUE.equals(mine)) {
            return false;
        }
        Pending p = jdbc.query("""
                SELECT id, topic, key, payload::text AS payload FROM outbox
                WHERE published_at IS NULL ORDER BY id LIMIT 1""",
                (rs, i) -> new Pending(rs.getLong("id"), rs.getString("topic"), rs.getString("key"),
                        rs.getString("payload"))).stream().findFirst().orElse(null);
        if (p == null) {
            return false;
        }
        try {
            kafka.send(p.topic(), p.key(), p.payload()).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            warn(p, e);
            return false;
        }
        jdbc.update("UPDATE outbox SET published_at = now() WHERE id = ?", p.id());
        log.info("Published outbox message {} to {}", p.id(), p.topic());
        return true;
    }

    private void warn(Pending p, Exception e) {
        Instant now = clock.instant();
        if (Duration.between(lastWarning, now).compareTo(WARN_INTERVAL) >= 0) {
            lastWarning = now;
            log.warn("Could not publish outbox message {} to {}; retrying every second: {}", p.id(), p.topic(),
                    e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        }
    }
}
