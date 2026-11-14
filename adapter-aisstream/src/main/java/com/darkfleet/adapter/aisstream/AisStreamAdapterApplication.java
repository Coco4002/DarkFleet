package com.darkfleet.adapter.aisstream;

import com.darkfleet.adapter.support.FeedPublisher;
import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.AisSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
@EnableConfigurationProperties(AisStreamProperties.class)
public class AisStreamAdapterApplication {

    public static void main(String[] args) {
        SpringApplication.run(AisStreamAdapterApplication.class, args);
    }

    @Bean
    FeedPublisher feedPublisher(KafkaTemplate<String, Object> darkFleetKafkaTemplate) {
        return new FeedPublisher(AisSource.AISSTREAM, DarkFleetTopics.AIS_RAW, darkFleetKafkaTemplate);
    }
}
