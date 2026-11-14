package com.darkfleet.adapter.support;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes one feed's normalized messages to Kafka, keyed by MMSI.
 * <ul>
 *   <li>Drops exact repeats (identical content, apart from ingestion time) seen within a short window;
 *       feeds deliver the same report from several receivers. Two different reports in the same
 *       second are both kept.</li>
 *   <li>Counts a message as published only when Kafka acknowledges it. Failures are counted and
 *       logged (at most once a minute), and a failed message is forgotten by the repeat filter so a
 *       later retry is not mistaken for a repeat.</li>
 * </ul>
 */
public class FeedPublisher {

    private static final Logger log = LoggerFactory.getLogger(FeedPublisher.class);
    private static final Duration FAILURE_LOG_INTERVAL = Duration.ofMinutes(1);

    private final AisSource source;
    private final String topic;
    private final KafkaTemplate<String, Object> kafka;
    private final Clock clock;
    private final Duration dedupWindow;
    private final int dedupCapacity;

    /** Recently accepted messages (content without ingestion time), compared by full equality. */
    private final Map<NormalizedAisMessage, Instant> recentMessages = new LinkedHashMap<>(16, 0.75f, false);

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong sendFailures = new AtomicLong();
    private final AtomicLong messagesSinceHeartbeat = new AtomicLong();
    private final AtomicLong positionsSinceHeartbeat = new AtomicLong();
    private final AtomicReference<Instant> lastMessageAt = new AtomicReference<>();
    private final AtomicReference<Instant> lastFailureLogAt = new AtomicReference<>(Instant.EPOCH);

    public FeedPublisher(AisSource source, String topic, KafkaTemplate<String, Object> kafka) {
        this(source, topic, kafka, Clock.systemUTC(), Duration.ofMinutes(10), 100_000);
    }

    public FeedPublisher(AisSource source, String topic, KafkaTemplate<String, Object> kafka,
                         Clock clock, Duration dedupWindow, int dedupCapacity) {
        this.source = source;
        this.topic = topic;
        this.kafka = kafka;
        this.clock = clock;
        this.dedupWindow = dedupWindow;
        this.dedupCapacity = dedupCapacity;
    }

    /**
     * @return true if the message was handed to Kafka, false if it was a repeat or the send could
     *         not even be started
     */
    public boolean publish(NormalizedAisMessage message) {
        received.incrementAndGet();
        Instant now = clock.instant();
        NormalizedAisMessage key = message.withoutIngestionTime();
        if (!reserve(key, now)) {
            duplicates.incrementAndGet();
            return false;
        }
        lastMessageAt.set(now);
        messagesSinceHeartbeat.incrementAndGet();
        if (message.messageType() == AisMessageType.POSITION_REPORT) {
            positionsSinceHeartbeat.incrementAndGet();
        }
        sent.incrementAndGet();
        try {
            kafka.send(topic, message.mmsi(), message).whenComplete((result, error) -> {
                if (error == null) {
                    published.incrementAndGet();
                } else {
                    release(key);
                    recordFailure(error);
                }
            });
            return true;
        } catch (RuntimeException e) {
            // e.g. broker unreachable for longer than max.block.ms
            release(key);
            recordFailure(e);
            return false;
        }
    }

    /**
     * Waits until every message handed to Kafka has been acknowledged or has failed.
     *
     * @return true if everything settled within the timeout
     */
    public boolean awaitSettled(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (published.get() + sendFailures.get() < sent.get()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    private synchronized boolean reserve(NormalizedAisMessage key, Instant now) {
        Instant cutoff = now.minus(dedupWindow);
        var oldest = recentMessages.entrySet().iterator();
        while (oldest.hasNext()) {
            var entry = oldest.next();
            if (entry.getValue().isBefore(cutoff) || recentMessages.size() >= dedupCapacity) {
                oldest.remove();
            } else {
                break;
            }
        }
        return recentMessages.putIfAbsent(key, now) == null;
    }

    private synchronized void release(NormalizedAisMessage key) {
        recentMessages.remove(key);
    }

    private void recordFailure(Throwable error) {
        long failures = sendFailures.incrementAndGet();
        Instant now = clock.instant();
        Instant last = lastFailureLogAt.get();
        if (Duration.between(last, now).compareTo(FAILURE_LOG_INTERVAL) >= 0 && lastFailureLogAt.compareAndSet(last, now)) {
            log.warn("{}: Kafka send to {} failed ({} failures so far): {}", source, topic, failures, error.toString());
        }
    }

    public AisSource source() {
        return source;
    }

    public String topic() {
        return topic;
    }

    /** Messages accepted since the previous call (for heartbeats); resets the counters. */
    public long[] drainIntervalCounts() {
        return new long[]{messagesSinceHeartbeat.getAndSet(0), positionsSinceHeartbeat.getAndSet(0)};
    }

    public FeedStats stats() {
        return new FeedStats(received.get(), duplicates.get(), sent.get(), published.get(), sendFailures.get(),
                lastMessageAt.get());
    }

    /**
     * @param received      messages offered by the feed
     * @param duplicates    of those, dropped as exact repeats
     * @param sent          handed to Kafka
     * @param published     acknowledged by Kafka
     * @param sendFailures  sends that failed
     * @param lastMessageAt when the last accepted message arrived, null if none yet
     */
    public record FeedStats(long received, long duplicates, long sent, long published, long sendFailures,
                            Instant lastMessageAt) {
    }
}
