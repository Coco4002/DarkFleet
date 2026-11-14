package com.darkfleet.adapter.digitraffic;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/adapter/digitraffic")
@Tag(name = "Fintraffic Digitraffic feed adapter",
        description = "Live AIS over MQTT for Finnish waters (Fintraffic / digitraffic.fi, CC BY 4.0)")
public class DigitrafficController {

    private final DigitrafficFeedService feedService;

    public DigitrafficController(DigitrafficFeedService feedService) {
        this.feedService = feedService;
    }

    @GetMapping("/status")
    @Operation(summary = "Connection state, attribution and message counters")
    public Map<String, Object> status() {
        return feedService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")
    @PostMapping("/disconnect")
    @Operation(summary = "Disconnect the feed to demonstrate a single-feed outage (admin)")
    public Map<String, Object> disconnect() {
        feedService.simulateDisconnect();
        return feedService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")
    @PostMapping("/reconnect")
    @Operation(summary = "Reconnect the feed after a simulated outage (admin)")
    public Map<String, Object> reconnect() {
        feedService.reconnect();
        return feedService.getStatus();
    }
}
