package com.darkfleet.adapter.digitraffic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DigitrafficAdapterApplication {

    public static void main(String[] args) {
        SpringApplication.run(DigitrafficAdapterApplication.class, args);
    }
}
