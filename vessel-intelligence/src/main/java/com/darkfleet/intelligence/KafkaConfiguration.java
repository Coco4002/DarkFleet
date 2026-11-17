package com.darkfleet.intelligence;

import com.darkfleet.common.kafka.DarkFleetTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Duration;
import java.util.Arrays;

@Configuration(proxyBeanMethods = false)
class KafkaConfiguration {

    /** Referenced by name from the {@code @KafkaListener} topic expressions. */
    @Bean
    IntelligenceTopics intelTopics(IntelligenceProperties properties) {
        return IntelligenceTopics.of(properties);
    }

    /** The topics read here, declared with the project's settings in case this service starts first. */
    @Bean
    KafkaAdmin.NewTopics intelligenceTopicDeclarations(IntelligenceTopics topics) {
        return new KafkaAdmin.NewTopics(Arrays.stream(topics.all()).map(DarkFleetTopics::newTopic).toArray(NewTopic[]::new));
    }

    /**
     * A batch that can't be stored (typically the database is down) is retried, waiting longer each
     * time up to a minute, until it succeeds. Nothing is skipped: offsets are committed only after the
     * rows are, so giving up would lose data. Messages that can't be read at all never get here; the
     * listeners skip them.
     */
    @Bean
    CommonErrorHandler retryUntilStored() {
        ExponentialBackOff backOff = new ExponentialBackOff(Duration.ofSeconds(1).toMillis(), 2.0);
        backOff.setMaxInterval(Duration.ofMinutes(1).toMillis());
        return new DefaultErrorHandler(backOff);
    }
}
