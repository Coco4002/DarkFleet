package com.darkfleet.adapter.support;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.FeedHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Publishes a {@link FeedHealth} heartbeat for a live feed every interval, keyed by source.
 */
public class FeedHealthReporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FeedHealthReporter.class);

    private final FeedPublisher publisher;
    private final FeedConnection connection;
    private final KafkaTemplate<String, Object> kafka;
    private final Duration interval;
    private final Clock clock;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "feed-health");
        t.setDaemon(true);
        return t;
    });

    public FeedHealthReporter(FeedPublisher publisher, FeedConnection connection,
                              KafkaTemplate<String, Object> kafka, Duration interval, Clock clock) {
        this.publisher = publisher;
        this.connection = connection;
        this.kafka = kafka;
        this.interval = interval;
        this.clock = clock;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        long millis = interval.toMillis();
        scheduler.scheduleAtFixedRate(this::report, millis, millis, TimeUnit.MILLISECONDS);
    }

    FeedHealth snapshot() {
        long[] interval = publisher.drainIntervalCounts();
        var stats = publisher.stats();
        return new FeedHealth(
                publisher.source(),
                clock.instant(),
                (int) this.interval.toSeconds(),
                connection.isConnected(),
                interval[0],
                interval[1],
                stats.lastMessageAt(),
                stats.published(),
                stats.sendFailures(),
                connection.totalReconnects()
        );
    }

    void report() {
        try {
            FeedHealth health = snapshot();
            kafka.send(DarkFleetTopics.FEED_HEALTH, health.source().name(), health).whenComplete((result, error) -> {
                if (error != null) {
                    log.warn("Feed health for {} was not delivered: {}", publisher.source(), error.toString());
                }
            });
        } catch (RuntimeException e) {
            log.warn("Could not publish feed health for {}: {}", publisher.source(), e.toString());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
