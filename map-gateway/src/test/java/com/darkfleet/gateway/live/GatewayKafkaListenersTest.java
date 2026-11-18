package com.darkfleet.gateway.live;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.GatewayTopics;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GatewayKafkaListenersTest {

    private static final Instant NOW = Instant.parse("2026-11-18T12:00:00Z");

    @Test
    void seeksStaticAndFeedHealthToBeginningAndTrackPointsOneHourBackInLiveMode() {
        GatewayProperties props = new GatewayProperties(false, Duration.ofHours(1), Duration.ofSeconds(5),
                Duration.ofMinutes(2), "http://localhost:8086", "", Duration.ofSeconds(5), "", Duration.ofSeconds(5),
                Duration.ofHours(1));
        GatewayTopics topics = GatewayTopics.of(props);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        LiveSnapshotStore store = new LiveSnapshotStore(props, clock);
        GatewayKafkaListeners listeners = new GatewayKafkaListeners(store, props, topics, clock);

        TopicPartition staticTp = new TopicPartition(DarkFleetTopics.VESSEL_STATIC, 0);
        TopicPartition trackTp = new TopicPartition(DarkFleetTopics.TRACK_POINTS, 0);
        RecordingSeekCallback callback = new RecordingSeekCallback();

        listeners.onPartitionsAssigned(Map.of(staticTp, 0L, trackTp, 0L), callback);

        assertEquals(List.of(staticTp), callback.beginning);
        assertEquals(List.of(trackTp), callback.timestampPartitions);
        assertEquals(NOW.minus(Duration.ofHours(1)).toEpochMilli(), callback.timestamp);
    }

    @Test
    void seeksReplayTrackPointsToBeginningInReplayMode() {
        GatewayProperties props = new GatewayProperties(true, Duration.ofHours(1), Duration.ofSeconds(5),
                Duration.ofMinutes(2), "http://localhost:8087", "", Duration.ofSeconds(5), "", Duration.ofSeconds(5),
                Duration.ofHours(1));
        GatewayTopics topics = GatewayTopics.of(props);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        LiveSnapshotStore store = new LiveSnapshotStore(props, clock);
        GatewayKafkaListeners listeners = new GatewayKafkaListeners(store, props, topics, clock);

        TopicPartition replayTrackTp = new TopicPartition(DarkFleetTopics.output(DarkFleetTopics.TRACK_POINTS, true), 0);
        RecordingSeekCallback callback = new RecordingSeekCallback();

        listeners.onPartitionsAssigned(Map.of(replayTrackTp, 0L), callback);

        assertEquals(List.of(replayTrackTp), callback.beginning);
        assertEquals(List.of(), callback.timestampPartitions);
    }

    private static final class RecordingSeekCallback implements ConsumerSeekCallback {
        final List<TopicPartition> beginning = new ArrayList<>();
        final List<TopicPartition> timestampPartitions = new ArrayList<>();
        long timestamp;

        @Override
        public void seek(String topic, int partition, long offset) {
        }

        @Override
        public void seek(String topic, int partition, Function<Long, Long> offsetCompute) {
        }

        @Override
        public void seekToBeginning(String topic, int partition) {
            beginning.add(new TopicPartition(topic, partition));
        }

        @Override
        public void seekToBeginning(Collection<TopicPartition> partitions) {
            beginning.addAll(partitions);
        }

        @Override
        public void seekToEnd(String topic, int partition) {
        }

        @Override
        public void seekRelative(String topic, int partition, long offset, boolean toCurrent) {
        }

        @Override
        public void seekToTimestamp(String topic, int partition, long timestamp) {
            timestampPartitions.add(new TopicPartition(topic, partition));
            this.timestamp = timestamp;
        }

        @Override
        public void seekToTimestamp(Collection<TopicPartition> partitions, long timestamp) {
            timestampPartitions.addAll(partitions);
            this.timestamp = timestamp;
        }
    }
}
