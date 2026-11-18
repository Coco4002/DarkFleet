package com.darkfleet.gateway;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.FeedHealth;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.gateway.live.LiveSnapshotStore;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The gateway against a real Kafka broker: the live state is built from the topics, and a start reads
 * {@code track.points} from one hour back only, even on a partition that holds nothing that recent.
 */
@SpringBootTest(properties = {
        "darkfleet.gateway.intelligence-url=http://127.0.0.1:9",
        "darkfleet.gateway.intelligence-api-key=test-gateway-key"
})
class MapGatewayIntegrationTest {

    private static final String MMSI = "230123456";
    private static final String EXPIRED_MMSI = "273999999";
    private static final String OLD_RECORD_MMSI = "265000111";
    private static final String CELL = "861126d37ffffff";
    /** The partition holding only an old record before the gateway starts. */
    private static final int IDLE_PARTITION = 5;

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");
    private static final KafkaProducer<String, byte[]> PRODUCER;

    static {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(Stream.of(DarkFleetTopics.VESSEL_STATIC, DarkFleetTopics.TRACK_POINTS,
                    DarkFleetTopics.FEED_HEALTH).map(DarkFleetTopics::newTopic).toList()).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        PRODUCER = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"),
                new StringSerializer(), new ByteArraySerializer());
        // Written two hours ago by the broker's clock, but its position looks current: if the gateway read
        // this partition from the beginning, the ship would show up on the live map.
        Instant now = Instant.now();
        send(DarkFleetTopics.TRACK_POINTS, IDLE_PARTITION, now.minus(Duration.ofHours(2)), OLD_RECORD_MMSI,
                new TrackPoint(OLD_RECORD_MMSI, now, 59.4, 24.7, 10.0, 90.0, 90.0, 0, AisSource.AISSTREAM, CELL));
    }

    @AfterAll
    static void closeProducer() {
        PRODUCER.close();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private LiveSnapshotStore store;

    @Test
    void buildsTheLiveStateFromKafkaAndRewindsTrackPointsOnlyOneHour() {
        Instant now = Instant.now();
        send(DarkFleetTopics.VESSEL_STATIC, null, now, MMSI, new VesselStatic(MMSI, now.minusSeconds(30),
                AisSource.DIGITRAFFIC, "9308065", "BALTIC EXPRESS", "OJBX", 80, 11.4, "PRIMORSK", 244, 42, null));
        send(DarkFleetTopics.TRACK_POINTS, null, now, EXPIRED_MMSI, new TrackPoint(EXPIRED_MMSI,
                now.minus(Duration.ofHours(2)), 59.5, 24.0, 8.0, 180.0, 180.0, 0, AisSource.AISSTREAM, CELL));
        // behind the old record in the same partition: once this arrives, the old one would have been read
        send(DarkFleetTopics.TRACK_POINTS, IDLE_PARTITION, now, MMSI, new TrackPoint(MMSI, now.minusSeconds(10),
                60.10, 24.95, 12.1, 90.0, 90.0, 0, AisSource.DIGITRAFFIC, CELL));
        send(DarkFleetTopics.FEED_HEALTH, null, now, AisSource.DIGITRAFFIC.name(), new FeedHealth(
                AisSource.DIGITRAFFIC, now.minusSeconds(5), 30, true, 1050, 1000, now.minusSeconds(1), 50000, 0, 0));

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .until(() -> store.latestPosition(MMSI) != null && store.latestPosition(EXPIRED_MMSI) != null
                        && store.staticData(MMSI) != null && !store.feeds().isEmpty());

        assertNull(store.latestPosition(OLD_RECORD_MMSI), "a record older than an hour was read on start");
        List<String> live = store.snapshot().vessels().stream().map(LiveSnapshotStore.LiveVesselView::mmsi).toList();
        assertEquals(List.of(MMSI), live);
        assertEquals("BALTIC EXPRESS", store.snapshot().vessels().getFirst().name());
        assertFalse(store.feeds().getFirst().stale());
    }

    private static void send(String topic, Integer partition, Instant timestamp, String key, Object payload) {
        try {
            byte[] bytes = JsonSerde.mapper().writeValueAsBytes(payload);
            PRODUCER.send(new ProducerRecord<>(topic, partition, timestamp.toEpochMilli(), key, bytes)).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
