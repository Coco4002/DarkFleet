package com.darkfleet.gateway.live;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.FeedHealth;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.GatewayTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Populates {@link LiveSnapshotStore} from Kafka and recovers state on restart without a database:
 * <ul>
 *   <li>{@code vessel.static} and {@code feed.health} are seeked to the beginning on every partition assignment,
 *       so every active vessel's identity and every live adapter's last heartbeat are restored.</li>
 *   <li>{@code track.points} is seeked via {@code seekToTimestamp(now - vesselExpiry)} (~1 hour back) in live mode
 *       (or to the beginning in replay mode, where replayed timestamps are historical), so the live map recovers
 *       within seconds of a gateway restart without loading 7 days of positions.</li>
 * </ul>
 * Every instance reads every partition (its own random consumer group) and commits nothing.
 */
@Component
class GatewayKafkaListeners implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(GatewayKafkaListeners.class);

    private final LiveSnapshotStore store;
    private final GatewayProperties properties;
    private final GatewayTopics topics;
    private final Clock clock;

    GatewayKafkaListeners(LiveSnapshotStore store, GatewayProperties properties, GatewayTopics topics, Clock clock) {
        this.store = store;
        this.properties = properties;
        this.topics = topics;
        this.clock = clock;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (assignments.isEmpty()) {
            return;
        }
        List<TopicPartition> fromBeginning = new ArrayList<>();
        List<TopicPartition> trackPartitions = new ArrayList<>();
        for (TopicPartition tp : assignments.keySet()) {
            if (tp.topic().equals(topics.trackPoints())) {
                trackPartitions.add(tp);
            } else {
                fromBeginning.add(tp);
            }
        }
        if (!fromBeginning.isEmpty()) {
            callback.seekToBeginning(fromBeginning);
        }
        if (trackPartitions.isEmpty()) {
            return;
        }
        if (properties.replay()) {
            callback.seekToBeginning(trackPartitions);
            return;
        }
        Instant since = clock.instant().minus(properties.vesselExpiry());
        // A partition with nothing newer than the cutoff is left where it was by the timestamp seek
        // (here: the beginning), so move it to the end first. Seeks are applied in order.
        callback.seekToEnd(trackPartitions);
        callback.seekToTimestamp(trackPartitions, since.toEpochMilli());
    }

    @KafkaListener(
            topics = "#{@gatewayTopics.vesselStatic()}",
            groupId = "darkfleet-map-gateway-static-#{T(java.util.UUID).randomUUID()}"
    )
    void onVesselStatic(List<ConsumerRecord<String, byte[]>> records) {
        for (VesselStatic report : decode(records, VesselStatic.class)) {
            store.onVesselStatic(report);
        }
    }

    @KafkaListener(
            topics = "#{@gatewayTopics.trackPoints()}",
            groupId = "darkfleet-map-gateway-tracks-#{T(java.util.UUID).randomUUID()}"
    )
    void onTrackPoints(List<ConsumerRecord<String, byte[]>> records) {
        for (TrackPoint point : decode(records, TrackPoint.class)) {
            store.onTrackPoint(point);
        }
    }

    @KafkaListener(
            topics = "#{@gatewayTopics.feedHealth()}",
            groupId = "darkfleet-map-gateway-feeds-#{T(java.util.UUID).randomUUID()}"
    )
    void onFeedHealth(List<ConsumerRecord<String, byte[]>> records) {
        for (FeedHealth health : decode(records, FeedHealth.class)) {
            store.onFeedHealth(health);
        }
    }

    static <T> List<T> decode(List<ConsumerRecord<String, byte[]>> records, Class<T> type) {
        List<T> decoded = new ArrayList<>(records.size());
        for (ConsumerRecord<String, byte[]> record : records) {
            if (record.value() == null || record.value().length == 0) {
                continue;
            }
            try {
                T value = JsonSerde.mapper().readValue(record.value(), type);
                if (value == null) {
                    log.warn("Skipping null {} at {}-{} offset {}", type.getSimpleName(), record.topic(),
                            record.partition(), record.offset());
                    continue;
                }
                decoded.add(value);
            } catch (JacksonException e) {
                log.warn("Skipping unreadable {} at {}-{} offset {}: {}", type.getSimpleName(), record.topic(),
                        record.partition(), record.offset(), e.getOriginalMessage());
            }
        }
        return decoded;
    }
}
