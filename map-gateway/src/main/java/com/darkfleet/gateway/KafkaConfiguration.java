package com.darkfleet.gateway;

import com.darkfleet.common.kafka.DarkFleetTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Arrays;

@Configuration(proxyBeanMethods = false)
class KafkaConfiguration {

    @Bean
    GatewayTopics gatewayTopics(GatewayProperties properties) {
        return GatewayTopics.of(properties);
    }

    /** The topics read here, declared with the project's settings in case this service starts first. */
    @Bean
    KafkaAdmin.NewTopics gatewayTopicDeclarations(GatewayTopics topics) {
        return new KafkaAdmin.NewTopics(Arrays.stream(topics.all()).map(DarkFleetTopics::newTopic).toArray(NewTopic[]::new));
    }
}
