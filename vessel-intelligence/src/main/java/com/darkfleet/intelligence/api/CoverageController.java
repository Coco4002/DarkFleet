package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.read.CoverageQueries;
import com.darkfleet.intelligence.read.Views.CoverageView;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/coverage")
@PreAuthorize(Access.GATEWAY)
@Tag(name = "Coverage")
class CoverageController {

    private final CoverageQueries coverage;

    CoverageController(CoverageQueries coverage) {
        this.coverage = coverage;
    }

    @GetMapping
    @Operation(summary = "Reception per H3 cell (resolution 6) for the hour containing the given time")
    List<CoverageView> hour(@RequestParam Instant hour) {
        return coverage.hour(hour);
    }

    @GetMapping("/latest")
    @Operation(summary = "Reception per H3 cell for the newest published hour (hours are published after they end)")
    List<CoverageView> latest() {
        return coverage.latest();
    }
}
