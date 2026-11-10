package com.darkfleet.adapter.aisstream;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class AisStreamKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, NormalizedAisMessage> aisProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        JsonSerde<NormalizedAisMessage> serde = new JsonSerde<>(NormalizedAisMessage.class);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), serde.serializer());
    }

    @Bean
    public KafkaTemplate<String, NormalizedAisMessage> aisKafkaTemplate(
            ProducerFactory<String, NormalizedAisMessage> aisProducerFactory
    ) {
        return new KafkaTemplate<>(aisProducerFactory);
    }

    @Bean
    public NewTopic aisRawTopic() {
        return TopicBuilder.name(DarkFleetTopics.AIS_RAW)
                .partitions(6)
                .replicas(1)
                .build();
    }
}
