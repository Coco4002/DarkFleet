package com.darkfleet.intelligence;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class VesselIntelligenceApplication {

    public static void main(String[] args) {
        SpringApplication.run(VesselIntelligenceApplication.class, args);
    }

    /** The service's notion of now; tests replace it. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
