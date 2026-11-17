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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes the outbox to Kafka every second, oldest first, one message at a time and waiting for each
 * acknowledgement. On the first failure it stops and starts again from that message next round, so
 * messages are never skipped or reordered. A message may be published twice (sent, then the database
 * update failed); consumers of {@code rules.approved} keep the newest version, so that is harmless.
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
        transaction.executeWithoutResult(status -> {
            // one relay at a time, should a second instance ever run
            Boolean mine = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(hashtext('outbox relay ' || current_schema()))", Boolean.class);
            if (!Boolean.TRUE.equals(mine)) {
                return;
            }
            List<Pending> pending = jdbc.query("""
                    SELECT id, topic, key, payload::text AS payload FROM outbox
                    WHERE published_at IS NULL ORDER BY id LIMIT ?""",
                    (rs, i) -> new Pending(rs.getLong("id"), rs.getString("topic"), rs.getString("key"),
                            rs.getString("payload")), BATCH);
            for (Pending p : pending) {
                try {
                    kafka.send(p.topic(), p.key(), p.payload()).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (ExecutionException | TimeoutException | RuntimeException e) {
                    warn(p, e);
                    return;
                }
                jdbc.update("UPDATE outbox SET published_at = now() WHERE id = ?", p.id());
                log.info("Published outbox message {} to {}", p.id(), p.topic());
            }
        });
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
