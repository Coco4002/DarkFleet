package com.darkfleet.detection;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.stream.Stream;

@Configuration(proxyBeanMethods = false)
class DetectionConfiguration {

    private static final Logger log = LoggerFactory.getLogger(DetectionConfiguration.class);

    @Bean
    KStream<String, NormalizedAisMessage> detectionTopology(StreamsBuilder builder, DetectionProperties properties) {
        return DetectionTopology.build(builder, properties);
    }

    /** Every topic the topology reads or writes, created at startup before the streams start. */
    @Bean
    KafkaAdmin.NewTopics detectionTopics(DetectionProperties properties) {
        DetectionMode mode = properties.mode();
        return new KafkaAdmin.NewTopics(Stream.of(
                        mode.inputTopic(), DarkFleetTopics.FEED_HEALTH, DarkFleetTopics.RULES_APPROVED,
                        mode.output(DarkFleetTopics.VESSEL_STATIC), mode.output(DarkFleetTopics.TRACK_POINTS),
                        mode.output(DarkFleetTopics.COVERAGE_CELLS), mode.output(DarkFleetTopics.DETECTIONS))
                .map(DarkFleetTopics::newTopic).toArray(NewTopic[]::new));
    }

    /**
     * When processing fails for good (Kafka Streams goes to ERROR), the service exits, so the
     * container's restart policy starts it again instead of it sitting there "up" but doing nothing.
     */
    @Bean
    StreamsBuilderFactoryBeanConfigurer exitWhenStreamsFail(ConfigurableApplicationContext context) {
        return factory -> factory.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                log.error("Kafka Streams stopped after an error; exiting so the service gets restarted");
                // not on the streams thread: closing the context waits for that thread to finish
                new Thread(() -> System.exit(SpringApplication.exit(context, () -> 1)), "exit-on-streams-error").start();
            }
        });
    }

    /** Up while the streams are running or rebalancing; down when they failed or stopped. */
    @Bean
    HealthIndicator kafkaStreamsHealth(StreamsBuilderFactoryBean streams) {
        return () -> {
            KafkaStreams kafkaStreams = streams.getKafkaStreams();
            if (kafkaStreams == null) {
                return Health.down().withDetail("state", "not started").build();
            }
            KafkaStreams.State state = kafkaStreams.state();
            return (state.isRunningOrRebalancing() ? Health.up() : Health.down()).withDetail("state", state).build();
        };
    }
}
