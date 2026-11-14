package com.darkfleet.adapter.digitraffic;

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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/adapter/digitraffic")
@CrossOrigin(origins = "*")
@Tag(name = "Fintraffic Digitraffic Adapter", description = "Live AIS MQTT/REST adapter for Gulf of Finland (CC BY 4.0: Fintraffic / digitraffic.fi)")
public class DigitrafficController {

    private final DigitrafficFeedService feedService;

    public DigitrafficController(DigitrafficFeedService feedService) {
        this.feedService = feedService;
    }

    @GetMapping("/status")
    @Operation(summary = "Get Fintraffic Digitraffic adapter status, CC BY 4.0 attribution, and counters")
    public Map<String, Object> status() {
        return feedService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/disconnect")
    @Operation(summary = "Simulate a Digitraffic feed outage while keeping other feeds active")
    public Map<String, Object> disconnect() {
        feedService.simulateDisconnect();
        return feedService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/reconnect")
    @Operation(summary = "Reconnect the Digitraffic MQTT feed")
    public Map<String, Object> reconnect() {
        feedService.reconnect();
        return feedService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/poll-rest-snapshot")
    @Operation(summary = "Fetch latest Gulf of Finland vessel locations from Digitraffic REST API")
    public Map<String, Object> pollSnapshot() {
        int count = feedService.pollRestLocationsSnapshot();
        return Map.of("publishedFromSnapshot", count, "attribution", DigitrafficFeedService.ATTRIBUTION);
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/ingest-mqtt")
    @Operation(summary = "Ingest a Digitraffic MQTT message (vessels-v2/<mmsi>/location or metadata)")
    public ResponseEntity<?> ingestMqtt(
            @RequestParam("topic") String topic,
            @RequestBody String payloadJson
    ) {
        return feedService.handleMqttMessage(topic, payloadJson)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.badRequest().body(Map.of(
                        "accepted", false,
                        "reason", "Invalid topic/payload or Digitraffic adapter is disconnected"
                )));
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/publish-normalized")
    @Operation(summary = "Publish a normalized message through the Digitraffic adapter")
    public ResponseEntity<Map<String, Object>> publishNormalized(@RequestBody NormalizedAisMessage message) {
        boolean published = feedService.publishNormalized(message);
        return ResponseEntity.ok(Map.of("published", published, "mmsi", message.mmsi()));
    }
}
