package com.darkfleet.adapter.replay;

import com.darkfleet.common.model.NormalizedAisMessage;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/adapter/replay")
@CrossOrigin(origins = "*")
@Tag(name = "DMA Historical CSV & Scenario Replay Adapter", description = "Replays Danish Maritime Authority CSV files and recorded Baltic/Bosphorus scenarios (always marked historical)")
public class ReplayController {

    private final ReplayScenarioService scenarioService;

    public ReplayController(ReplayScenarioService scenarioService) {
        this.scenarioService = scenarioService;
    }

    @GetMapping("/status")
    @Operation(summary = "Get DMA replay adapter status and total historical messages replayed")
    public Map<String, Object> status() {
        return scenarioService.getStatus();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/run-corridor-suite")
    @Operation(summary = "Replay the recorded Baltic & Bosphorus multi-feed corridor scenario suite through Kafka")
    public Map<String, Object> runCorridorSuite() {
        int published = scenarioService.replayRecordedCorridorSuite();
        return Map.of(
                "status", "REPLAYED",
                "historical", true,
                "messagesPublished", published
        );
    }

    @GetMapping("/preview-corridor-suite")
    @Operation(summary = "Preview the normalized messages in the recorded corridor suite without publishing")
    public List<NormalizedAisMessage> previewCorridorSuite() {
        return scenarioService.buildRecordedCorridorSuite(Instant.now());
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping(value = "/ingest-dma-csv", consumes = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "Parse and replay Danish Maritime Authority (DMA) CSV lines (marked historical = true)")
    public Map<String, Object> ingestDmaCsv(@RequestBody String csvContent) {
        int count = scenarioService.replayCsv(csvContent);
        return Map.of(
                "status", "CSV_REPLAYED",
                "historical", true,
                "messagesPublished", count
        );
    }
}
