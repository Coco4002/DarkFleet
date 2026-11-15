package com.darkfleet.detection;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
@ConfigurationPropertiesScan
public class TrackDetectionApplication {

    public static void main(String[] args) {
        SpringApplication.run(TrackDetectionApplication.class, args);
    }
}
