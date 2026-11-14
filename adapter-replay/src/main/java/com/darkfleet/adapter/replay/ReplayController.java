package com.darkfleet.adapter.replay;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/adapter/replay")
@Tag(name = "DMA replay adapter",
        description = "Replays recorded Danish Maritime Authority AIS files into ais.replay (development and tests only)")
public class ReplayController {

    private final ReplayRunService replay;

    public ReplayController(ReplayRunService replay) {
        this.replay = replay;
    }

    @GetMapping("/status")
    @Operation(summary = "State and message counters of the current or last replay run")
    public ReplayRunService.RunStatus status() {
        return replay.status();
    }

    @PreAuthorize("hasRole('ADMIN')")
    @GetMapping("/files")
    @Operation(summary = "Replay files available in the data directory (admin)")
    public List<String> files() throws IOException {
        return replay.listFiles();
    }

    @PreAuthorize("hasRole('ADMIN')")
    @PostMapping("/runs")
    @Operation(summary = "Start replaying a file (admin); one run at a time")
    public ResponseEntity<ReplayRunService.RunStatus> start(@RequestBody ReplayRunService.ReplayRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(replay.start(request));
    }

    @PreAuthorize("hasRole('ADMIN')")
    @PostMapping("/runs/cancel")
    @Operation(summary = "Stop the running replay (admin)")
    public ReplayRunService.RunStatus cancel() {
        replay.cancel();
        return replay.status();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    ResponseEntity<Map<String, String>> conflict(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }
}
