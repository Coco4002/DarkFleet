package com.darkfleet.detection;

import com.darkfleet.common.model.CorridorRegion;
import com.darkfleet.common.model.CoverageCellHour;
import com.darkfleet.common.model.DetectionEvent;
import com.darkfleet.common.model.DetectionRuleConfig;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.VesselTrack;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@CrossOrigin(origins = "*")
@Tag(name = "DarkFleet Track & Detection API", description = "Streaming vessel tracks, replay windows, H3 coverage cells, and neutral candidate detections")
public class TrackDetectionController {

    private final TrackAndDetectionEngine engine;
    private final CoverageTracker coverageTracker;

    public TrackDetectionController(
            TrackAndDetectionEngine engine,
            CoverageTracker coverageTracker
    ) {
        this.engine = engine;
        this.coverageTracker = coverageTracker;
    }

    @GetMapping("/tracks")
    @Operation(summary = "List active vessel tracks, optionally filtered by corridor")
    public List<VesselTrack> listTracks(
            @RequestParam(value = "corridor", required = false) CorridorRegion corridor
    ) {
        return engine.listTracks(corridor);
    }

    @GetMapping("/tracks/{mmsi}")
    @Operation(summary = "Get reconstructed track and static identity history for a single vessel")
    public ResponseEntity<VesselTrack> getTrackByMmsi(@PathVariable("mmsi") String mmsi) {
        return engine.getTrack(mmsi)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/tracks/replay")
    @Operation(summary = "Replay vessel tracks within a past time window [from, to]")
    public List<VesselTrack> replayWindow(
            @RequestParam("from") Instant from,
            @RequestParam("to") Instant to
    ) {
        return engine.getTracksInWindow(from, to);
    }

    @GetMapping("/detections")
    @Operation(summary = "List candidate anomaly events with neutral wording")
    public List<DetectionEvent> listDetections(
            @RequestParam(value = "type", required = false) DetectionType type,
            @RequestParam(value = "mmsi", required = false) String mmsi,
            @RequestParam(value = "corridor", required = false) CorridorRegion corridor,
            @RequestParam(value = "includeSuppressed", defaultValue = "false") boolean includeSuppressed
    ) {
        return engine.listDetections(type, mmsi, corridor, includeSuppressed);
    }

    @GetMapping("/detections/encounters")
    @Operation(summary = "Search ship-to-ship encounter candidates and correlated post-encounter draught changes")
    public List<DetectionEvent> searchEncounters(
            @RequestParam(value = "mmsi", required = false) String mmsi,
            @RequestParam(value = "corridor", required = false) CorridorRegion corridor
    ) {
        return engine.searchEncounters(mmsi, corridor);
    }

    @GetMapping("/coverage")
    @Operation(summary = "List measured coverage statistics per H3 grid cell and hour")
    public List<CoverageCellHour> listCoverage() {
        return coverageTracker.listAllCoverageCells();
    }

    @GetMapping("/coverage/{h3Cell}")
    @Operation(summary = "Get measured coverage for a specific H3 grid cell")
    public CoverageCellHour getCellCoverage(
            @PathVariable("h3Cell") String h3Cell,
            @RequestParam(value = "at", required = false) Instant at
    ) {
        return coverageTracker.getCellCoverage(h3Cell, at != null ? at : Instant.now());
    }

    @GetMapping("/rules")
    @Operation(summary = "Get active detection rule thresholds")
    public DetectionRuleConfig getRules() {
        return engine.getActiveRules();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/rules")
    @Operation(summary = "Apply updated detection rule thresholds")
    public DetectionRuleConfig updateRules(@RequestBody DetectionRuleConfig updated) {
        engine.updateRules(updated);
        return engine.getActiveRules();
    }

    @PreAuthorize("hasRole('ADMIN')")

    @PostMapping("/stream/ingest")
    @Operation(summary = "Process a batch of normalized AIS messages through the Track & Detection engine")
    public Map<String, Object> ingestBatch(@RequestBody List<NormalizedAisMessage> messages) {
        int newDetectionsCount = 0;
        for (NormalizedAisMessage msg : messages) {
            TrackAndDetectionEngine.ProcessingOutcome outcome = engine.processMessage(msg);
            newDetectionsCount += outcome.newDetections().size();
        }
        return Map.of(
                "processedMessages", messages.size(),
                "activeTracks", engine.listTracks(null).size(),
                "newDetectionsEmitted", newDetectionsCount
        );
    }
}
