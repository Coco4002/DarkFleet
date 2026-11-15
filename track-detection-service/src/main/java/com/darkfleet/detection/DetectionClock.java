package com.darkfleet.detection;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * When the processors run their periodic checks, and what "now" is for them.
 * <ul>
 *   <li>Live: every check interval of wall-clock time, now = the wall clock.</li>
 *   <li>Replay: now = the newest message time processed so far by this instance, checked whenever it has
 *       moved on by a check interval. One clock for all processors, so a partition that receives
 *       nothing (an encounter pair that has split up) still sees replayed time pass. Replay runs as a
 *       single instance.</li>
 * </ul>
 */
public final class DetectionClock {

    /** How often a replay processor looks at the replay clock, in real time. */
    private static final Duration REPLAY_POLL = Duration.ofSeconds(1);

    private final DetectionProperties properties;
    private final AtomicReference<Instant> replayTime = new AtomicReference<>();

    public DetectionClock(DetectionProperties properties) {
        this.properties = properties;
    }

    /** Called with the time of every message processed. */
    public void observed(Instant messageTime) {
        replayTime.accumulateAndGet(messageTime, (a, b) -> a == null || b.isAfter(a) ? b : a);
    }

    /** Now, for a processor: the wall clock live; in replay the newest message time, null before the first. */
    public Instant now(ProcessorContext<?, ?> context) {
        return properties.mode() == DetectionMode.LIVE ? Instant.ofEpochMilli(context.currentSystemTimeMs()) : replayTime.get();
    }

    public void schedule(ProcessorContext<?, ?> context, Consumer<Instant> check) {
        if (properties.mode() == DetectionMode.LIVE) {
            context.schedule(properties.checkInterval(), PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> check.accept(Instant.ofEpochMilli(timestamp)));
            return;
        }
        AtomicReference<Instant> lastCheck = new AtomicReference<>();
        context.schedule(REPLAY_POLL, PunctuationType.WALL_CLOCK_TIME, ignored -> {
            Instant now = replayTime.get();
            Instant last = lastCheck.get();
            if (now != null && (last == null || Duration.between(last, now).compareTo(properties.checkInterval()) >= 0)) {
                lastCheck.set(now);
                check.accept(now);
            }
        });
    }
}
