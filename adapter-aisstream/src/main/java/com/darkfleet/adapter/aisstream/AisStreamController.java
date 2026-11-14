package com.darkfleet.adapter.aisstream;

import com.darkfleet.common.model.NormalizedAisMessage;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/adapter/aisstream")
@CrossOrigin(origins = "*")
@Tag(name = "aisstream.io Feed Adapter", description = "Live AIS WebSocket ingestion (Baltic + Bosphorus), health status, and single-feed disconnect simulation")
public class AisStreamController {

    private final AisStreamFeedService feedService;

    public AisStreamController(AisStreamFeedService feedService) {
        this.feedService = feedService;
    }

    @GetMapping("/status")
    @Operation(summary = "Get aisstream.io adapter connection state, backoff counters, and throughput")
    public Map<String, Object> status() {
        return feedService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/disconnect")
    @Operation(summary = "Simulate an aisstream.io feed outage while keeping other feeds running")
    public Map<String, Object> disconnect() {
        feedService.simulateDisconnect();
        return feedService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/reconnect")
    @Operation(summary = "Restore the aisstream.io feed connection")
    public Map<String, Object> reconnect() {
        feedService.reconnect();
        return feedService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/ingest")
    @Operation(summary = "Ingest a raw aisstream.io JSON frame and publish normalized message to Kafka")
    public ResponseEntity<?> ingestRawFrame(@RequestBody String rawJson) {
        return feedService.handleRawFrame(rawJson)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.badRequest().body(Map.of(
                        "accepted", false,
                        "reason", "Frame ignored, invalid, or adapter is disconnected"
                )));
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/publish-normalized")
    @Operation(summary = "Publish a pre-normalized AIS message directly through the aisstream adapter")
    public ResponseEntity<Map<String, Object>> publishNormalized(@RequestBody NormalizedAisMessage message) {
        boolean published = feedService.publishNormalized(message);
        return ResponseEntity.ok(Map.of("published", published, "mmsi", message.mmsi()));
    }
}
