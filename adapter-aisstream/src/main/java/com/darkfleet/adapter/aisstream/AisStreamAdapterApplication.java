package com.darkfleet.adapter.aisstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AisStreamAdapterApplication {

    public static void main(String[] args) {
        SpringApplication.run(AisStreamAdapterApplication.class, args);
    }
}
