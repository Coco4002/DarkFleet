package com.darkfleet.adapter.support;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer, topics and feed-health reporting shared by the feed adapters.
 * Topics are declared here with explicit settings and created by Spring's KafkaAdmin at startup,
 * before the adapters start their feeds (they start on ApplicationReadyEvent).
 */
@AutoConfiguration(beforeName = "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration")
public class AdapterSupportAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "darkFleetKafkaTemplate")
    public KafkaTemplate<String, Object> darkFleetKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        // Don't block a feed's receive thread for a minute when Kafka is unreachable; fail and count it.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        var factory = new DefaultKafkaProducerFactory<String, Object>(
                props, new StringSerializer(), new JsonSerde<>(Object.class).serializer());
        return new KafkaTemplate<>(factory);
    }

    @Bean
    public NewTopic aisRawTopic() {
        return DarkFleetTopics.newTopic(DarkFleetTopics.AIS_RAW);
    }

    @Bean
    public NewTopic aisReplayTopic() {
        return DarkFleetTopics.newTopic(DarkFleetTopics.AIS_REPLAY);
    }

    @Bean
    public NewTopic feedHealthTopic() {
        return DarkFleetTopics.newTopic(DarkFleetTopics.FEED_HEALTH);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnBean({FeedPublisher.class, FeedConnection.class})
    @ConditionalOnMissingBean
    public FeedHealthReporter feedHealthReporter(
            FeedPublisher publisher,
            FeedConnection connection,
            KafkaTemplate<String, Object> darkFleetKafkaTemplate,
            @Value("${darkfleet.feed.health-interval}") Duration interval) {
        return new FeedHealthReporter(publisher, connection, darkFleetKafkaTemplate, interval, Clock.systemUTC());
    }
}
