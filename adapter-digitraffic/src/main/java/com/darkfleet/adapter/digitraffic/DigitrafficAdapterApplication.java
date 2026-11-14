package com.darkfleet.adapter.digitraffic;

import com.darkfleet.adapter.support.FeedPublisher;
import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.AisSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class DigitrafficAdapterApplication {

    public static void main(String[] args) {
        SpringApplication.run(DigitrafficAdapterApplication.class, args);
    }

    @Bean
    FeedPublisher feedPublisher(KafkaTemplate<String, Object> darkFleetKafkaTemplate) {
        return new FeedPublisher(AisSource.DIGITRAFFIC, DarkFleetTopics.AIS_RAW, darkFleetKafkaTemplate);
    }
}
