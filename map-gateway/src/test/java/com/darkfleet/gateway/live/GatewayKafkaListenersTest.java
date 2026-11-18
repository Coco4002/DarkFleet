package com.darkfleet.gateway.live;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.GatewayTopics;
import com.darkfleet.gateway.TestProperties;
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
        GatewayProperties props = TestProperties.gateway(false, "http://localhost/w/api.php");
        GatewayTopics topics = GatewayTopics.of(props);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        LiveSnapshotStore store = new LiveSnapshotStore(props, clock);
        GatewayKafkaListeners listeners = new GatewayKafkaListeners(store, props, topics, clock);

        TopicPartition staticTp = new TopicPartition(DarkFleetTopics.VESSEL_STATIC, 0);
        TopicPartition trackTp = new TopicPartition(DarkFleetTopics.TRACK_POINTS, 0);
        RecordingSeekCallback callback = new RecordingSeekCallback();

        listeners.onPartitionsAssigned(Map.of(staticTp, 0L, trackTp, 0L), callback);

        // track points: to the end first, so a partition with nothing in the last hour isn't read from the start
        long hourAgo = NOW.minus(Duration.ofHours(1)).toEpochMilli();
        assertEquals(List.of("beginning " + staticTp, "end " + trackTp, "timestamp " + trackTp + " " + hourAgo),
                callback.calls);
    }

    @Test
    void seeksReplayTrackPointsToBeginningInReplayMode() {
        GatewayProperties props = TestProperties.gateway(true, "http://localhost/w/api.php");
        GatewayTopics topics = GatewayTopics.of(props);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        LiveSnapshotStore store = new LiveSnapshotStore(props, clock);
        GatewayKafkaListeners listeners = new GatewayKafkaListeners(store, props, topics, clock);

        TopicPartition replayTrackTp = new TopicPartition(DarkFleetTopics.output(DarkFleetTopics.TRACK_POINTS, true), 0);
        RecordingSeekCallback callback = new RecordingSeekCallback();

        listeners.onPartitionsAssigned(Map.of(replayTrackTp, 0L), callback);

        assertEquals(List.of("beginning " + replayTrackTp), callback.calls);
    }

    /** Records the seeks in the order they are requested; the container applies them in that order. */
    private static final class RecordingSeekCallback implements ConsumerSeekCallback {
        final List<String> calls = new ArrayList<>();

        @Override
        public void seek(String topic, int partition, long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seek(String topic, int partition, Function<Long, Long> offsetCompute) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekToBeginning(String topic, int partition) {
            calls.add("beginning " + new TopicPartition(topic, partition));
        }

        @Override
        public void seekToEnd(String topic, int partition) {
            calls.add("end " + new TopicPartition(topic, partition));
        }

        @Override
        public void seekToBeginning(Collection<TopicPartition> partitions) {
            partitions.forEach(tp -> seekToBeginning(tp.topic(), tp.partition()));
        }

        @Override
        public void seekToEnd(Collection<TopicPartition> partitions) {
            partitions.forEach(tp -> seekToEnd(tp.topic(), tp.partition()));
        }

        @Override
        public void seekToTimestamp(Collection<TopicPartition> partitions, long timestamp) {
            partitions.forEach(tp -> seekToTimestamp(tp.topic(), tp.partition(), timestamp));
        }

        @Override
        public void seekRelative(String topic, int partition, long offset, boolean toCurrent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekToTimestamp(String topic, int partition, long timestamp) {
            calls.add("timestamp " + new TopicPartition(topic, partition) + " " + timestamp);
        }
    }
}
