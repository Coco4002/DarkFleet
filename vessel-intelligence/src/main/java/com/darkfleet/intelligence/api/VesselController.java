package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.read.VesselQueries;
import com.darkfleet.intelligence.read.Views.HistoryHourView;
import com.darkfleet.intelligence.read.Views.TrackPointView;
import com.darkfleet.intelligence.read.Views.VesselProfile;
import com.darkfleet.intelligence.read.Views.VesselSummary;
import com.darkfleet.intelligence.web.PageResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/vessels")
@PreAuthorize(Access.READ)
@Tag(name = "Vessels")
class VesselController {

    private final VesselQueries vessels;
    private final Clock clock;

    VesselController(VesselQueries vessels, Clock clock) {
        this.vessels = vessels;
        this.clock = clock;
    }

    @GetMapping
    @Operation(summary = "Search vessels by MMSI, IMO number or name; filter by sanction list or watchlist")
    PageResult<VesselSummary> search(@RequestParam(required = false) String q,
                                     @RequestParam(defaultValue = "false") boolean sanctioned,
                                     @RequestParam(defaultValue = "false") boolean listed,
                                     @RequestParam(defaultValue = "false") boolean watchlisted,
                                     @RequestParam(defaultValue = "0") int page,
                                     @RequestParam(defaultValue = "" + PageResult.DEFAULT_SIZE) int size) {
        return vessels.search(q, sanctioned, listed, watchlisted, page, size);
    }

    @GetMapping("/{mmsi}")
    @Operation(summary = "Full profile: data as broadcast, identity history, sanction lists, watchlist, detections, cases")
    VesselProfile profile(@PathVariable String mmsi) {
        return vessels.profile(mmsi);
    }

    @GetMapping("/{mmsi}/track")
    @PreAuthorize(Access.GATEWAY)
    @Operation(summary = "Track points in [from, to), at most 48 hours; defaults to the last 24 hours")
    List<TrackPointView> track(@PathVariable String mmsi,
                               @RequestParam(required = false) Instant from,
                               @RequestParam(required = false) Instant to) {
        Instant end = to != null ? to : clock.instant();
        Instant start = from != null ? from : end.minus(Duration.ofHours(24));
        return vessels.track(mmsi, start, end);
    }

    @GetMapping("/tracks")
    @PreAuthorize(Access.GATEWAY)
    @Operation(summary = "Coarse all-ships track history (1 point per 5 minutes) for the hour containing the given time")
    HistoryHourView historyHour(@RequestParam Instant hour) {
        return vessels.historyHour(hour);
    }
}
