package com.darkfleet.adapter.replay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Replays recorded DMA files into ais.replay (never ais.raw). Each run creates its own publisher;
 * see {@link ReplayRunService}.
 */
@SpringBootApplication
public class ReplayAdapterApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReplayAdapterApplication.class, args);
    }
}
