package com.darkfleet.gateway.api;

import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.gateway.flag.MmsiFlag;
import com.darkfleet.gateway.intelligence.IntelligenceClient;
import com.darkfleet.gateway.intelligence.IntelligenceClient.CoverageView;
import com.darkfleet.gateway.intelligence.IntelligenceClient.HistoryHourView;
import com.darkfleet.gateway.intelligence.IntelligenceClient.TrackPointView;
import com.darkfleet.gateway.live.LiveSnapshotStore;
import com.darkfleet.gateway.live.LiveSnapshotStore.FeedHealthView;
import com.darkfleet.gateway.live.LiveSnapshotStore.LiveSnapshot;
import com.darkfleet.gateway.live.LiveSnapshotStore.MergedStatic;
import com.darkfleet.gateway.live.LiveSnapshotStreamer;
import com.darkfleet.gateway.photo.WikimediaPhotoClient;
import com.darkfleet.gateway.photo.WikimediaPhotoClient.VesselPhoto;
import com.darkfleet.gateway.web.ApiException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Public Map")
public class PublicMapController {

    private static final CacheControl CURRENT_HOUR_CACHE = CacheControl.maxAge(Duration.ofSeconds(15)).cachePublic();
    private static final CacheControl PAST_HOUR_CACHE = CacheControl.maxAge(Duration.ofSeconds(60)).cachePublic();

    public record PublicVesselPanel(
            String mmsi,
            String name,
            String imo,
            String callSign,
            String flagCode,
            String flagCountry,
            Integer shipType,
            Integer lengthMeters,
            Integer beamMeters,
            Double draughtMeters,
            String destination,
            Instant eta,
            Instant staticReportedAt,
            TrackPointView lastPosition,
            List<TrackPointView> trail,
            VesselPhoto photo
    ) {
    }

    private final LiveSnapshotStore store;
    private final LiveSnapshotStreamer streamer;
    private final IntelligenceClient intelligence;
    private final WikimediaPhotoClient photos;
    private final Clock clock;

    public PublicMapController(LiveSnapshotStore store, LiveSnapshotStreamer streamer,
                               IntelligenceClient intelligence, WikimediaPhotoClient photos, Clock clock) {
        this.store = store;
        this.streamer = streamer;
        this.intelligence = intelligence;
        this.photos = photos;
        this.clock = clock;
    }

    @GetMapping(value = "/live", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Live SSE stream: full snapshot of all active vessels every ~5 s and a heartbeat every 20 s")
    public SseEmitter liveStream() {
        return streamer.connect();
    }

    @GetMapping("/snapshot")
    @Operation(summary = "Current live snapshot of all active vessels and live feed statuses")
    public LiveSnapshot snapshot() {
        return store.snapshot();
    }

    @GetMapping("/feeds")
    @Operation(summary = "Current live feed health and staleness status")
    public List<FeedHealthView> feeds() {
        return store.feeds();
    }

    @GetMapping("/vessels/{mmsi}")
    @Operation(summary = "Public ship panel: identity, flag, dimensions, voyage, last-24 h trail and Wikimedia Commons photo")
    public PublicVesselPanel vesselPanel(@PathVariable String mmsi) {
        IntelligenceClient.requireMmsi(mmsi);
        MergedStatic stat = store.staticData(mmsi);
        TrackPoint latest = store.latestPosition(mmsi);
        Instant now = clock.instant();
        List<TrackPointView> trail;
        try {
            trail = intelligence.vesselTrack(mmsi, now.minus(Duration.ofHours(24)), now);
        } catch (ApiException e) {
            if (stat == null && latest == null) {
                throw e;
            }
            trail = latest != null ? List.of(toTrackPointView(latest)) : List.of();
        }
        if (stat == null && latest == null && trail.isEmpty()) {
            throw ApiException.notFound("No vessel with MMSI " + mmsi + " has been seen");
        }
        TrackPointView lastPos = latest != null ? toTrackPointView(latest)
                : trail.isEmpty() ? null : trail.getLast();
        MmsiFlag.Flag flag = MmsiFlag.resolve(mmsi);
        String imo = stat == null ? null : stat.imo();
        VesselPhoto photo = photos.lookup(imo);
        return new PublicVesselPanel(
                mmsi,
                stat == null ? null : stat.name(),
                imo,
                stat == null ? null : stat.callSign(),
                flag == null ? null : flag.code(),
                flag == null ? null : flag.country(),
                stat == null ? null : stat.shipType(),
                stat == null ? null : stat.lengthMeters(),
                stat == null ? null : stat.beamMeters(),
                stat == null ? null : stat.draughtMeters(),
                stat == null ? null : stat.destination(),
                stat == null ? null : stat.eta(),
                stat == null ? null : stat.reportedAt(),
                lastPos,
                trail,
                photo
        );
    }

    @GetMapping("/vessels/{mmsi}/track")
    @Operation(summary = "Full 1-minute track of one vessel in [from, to), at most 48 hours; defaults to the last 24 hours")
    public List<TrackPointView> vesselTrack(@PathVariable String mmsi,
                                            @RequestParam(required = false) Instant from,
                                            @RequestParam(required = false) Instant to) {
        Instant end = to != null ? to : clock.instant();
        Instant start = from != null ? from : end.minus(Duration.ofHours(24));
        return intelligence.vesselTrack(mmsi, start, end);
    }

    @GetMapping("/history/tracks")
    @Operation(summary = "Coarse all-ships track history (1 point / 5 min) for the hour containing the given time")
    public ResponseEntity<HistoryHourView> historyHour(@RequestParam Instant hour) {
        Instant truncated = hour.truncatedTo(ChronoUnit.HOURS);
        HistoryHourView view = intelligence.historyHour(truncated);
        return ResponseEntity.ok()
                .cacheControl(cacheControlForHour(truncated))
                .body(view);
    }

    @GetMapping("/coverage")
    @Operation(summary = "Reception statistics per H3 cell (resolution 6) for the hour containing the given time")
    public ResponseEntity<List<CoverageView>> coverageHour(@RequestParam Instant hour) {
        Instant truncated = hour.truncatedTo(ChronoUnit.HOURS);
        List<CoverageView> cells = intelligence.coverageHour(truncated);
        return ResponseEntity.ok()
                .cacheControl(cacheControlForHour(truncated))
                .body(cells);
    }

    private CacheControl cacheControlForHour(Instant hourStart) {
        Instant currentHour = clock.instant().truncatedTo(ChronoUnit.HOURS);
        return hourStart.isBefore(currentHour) ? PAST_HOUR_CACHE : CURRENT_HOUR_CACHE;
    }

    private static TrackPointView toTrackPointView(TrackPoint p) {
        return new TrackPointView(p.time(), p.latitude(), p.longitude(), p.sogKnots(), p.cogDegrees(),
                p.headingDegrees(), p.navStatus(), p.source().name());
    }
}
