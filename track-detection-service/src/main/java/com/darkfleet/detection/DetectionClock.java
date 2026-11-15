package com.darkfleet.detection;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import java.time.Instant;
import java.util.function.Consumer;

/**
 * When the processors run their periodic checks, and what "now" is for them.
 * <ul>
 *   <li>Live: the wall clock, so a silent vessel is noticed even when nothing else arrives.</li>
 *   <li>Replay: each partition's stream time, i.e. the newest message time it has processed. A
 *       partition never runs ahead of its own unprocessed messages, however unevenly a fast replay
 *       is consumed. The cost: on a partition that receives nothing more (typically at the end of a
 *       replay) time stops, and what is still open there stays open.</li>
 * </ul>
 */
public final class DetectionClock {

    private final DetectionProperties properties;

    public DetectionClock(DetectionProperties properties) {
        this.properties = properties;
    }

    public Instant now(ProcessorContext<?, ?> context) {
        return Instant.ofEpochMilli(live() ? context.currentSystemTimeMs() : context.currentStreamTimeMs());
    }

    public void schedule(ProcessorContext<?, ?> context, Consumer<Instant> check) {
        context.schedule(properties.checkInterval(),
                live() ? PunctuationType.WALL_CLOCK_TIME : PunctuationType.STREAM_TIME,
                timestamp -> check.accept(Instant.ofEpochMilli(timestamp)));
    }

    private boolean live() {
        return properties.mode() == DetectionMode.LIVE;
    }
}
