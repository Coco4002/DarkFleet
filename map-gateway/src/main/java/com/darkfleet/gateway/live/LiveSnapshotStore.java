package com.darkfleet.gateway.live;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.FeedHealth;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.flag.MmsiFlag;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory live state built exclusively from Kafka events ({@code vessel.static}, {@code track.points},
 * {@code feed.health}). Vessels whose last position report is older than {@code vesselExpiry} (1 hour)
 * expire from the live snapshot; their static data stays for {@code staticExpiry} (the topic's retention)
 * so a ship that reports again later immediately carries its name, IMO number and dimensions.
 * Ages are measured against the wall clock, or in replay mode against the newest replayed position.
 */
@Component
public class LiveSnapshotStore {

    public record MergedStatic(
            String imo,
            String name,
            String callSign,
            Integer shipType,
            Integer lengthMeters,
            Integer beamMeters,
            Double draughtMeters,
            String destination,
            Instant eta,
            AisSource source,
            Instant reportedAt
    ) {
    }

    public record LiveVesselView(
            String mmsi,
            Instant time,
            double latitude,
            double longitude,
            Double sogKnots,
            Double cogDegrees,
            Double headingDegrees,
            Integer navStatus,
            AisSource source,
            String h3Cell,
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
            Instant eta
    ) {
    }

    public record FeedHealthView(
            AisSource source,
            boolean connected,
            boolean stale,
            Instant emittedAt,
            Instant lastMessageAt,
            long messagesInInterval,
            long positionsInInterval
    ) {
    }

    public record LiveSnapshot(
            Instant emittedAt,
            List<FeedHealthView> feeds,
            List<LiveVesselView> vessels
    ) {
    }

    private final GatewayProperties properties;
    private final Clock clock;
    private final Map<String, TrackPoint> latestPositions = new ConcurrentHashMap<>();
    private final Map<String, MergedStatic> staticByMmsi = new ConcurrentHashMap<>();
    private final Map<AisSource, FeedHealth> feedHealth = new ConcurrentHashMap<>();
    /** Newest position time seen; the replay clock. Starts at the epoch, so nothing expires before data arrives. */
    private final AtomicReference<Instant> newestPosition = new AtomicReference<>(Instant.EPOCH);

    public LiveSnapshotStore(GatewayProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    /** Records a track point if it is newer than what we already hold for that MMSI. */
    public void onTrackPoint(TrackPoint point) {
        if (point == null || !validCoordinates(point.latitude(), point.longitude())) {
            return;
        }
        latestPositions.compute(point.mmsi(), (mmsi, current) ->
                current == null || point.time().isAfter(current.time()) ? point : current);
        newestPosition.accumulateAndGet(point.time(), (a, b) -> a.isAfter(b) ? a : b);
    }

    /**
     * Merges a static vessel report: a newer report updates the fields it carries while keeping
     * non-null fields from earlier reports when the newer report leaves them blank. An out-of-order
     * older report only fills in fields that are still missing.
     */
    public void onVesselStatic(VesselStatic report) {
        if (report == null) {
            return;
        }
        staticByMmsi.compute(report.mmsi(), (mmsi, current) -> mergeStatic(current, report));
    }

    /** Records a feed heartbeat if it is at least as recent as the last one for that feed. */
    public void onFeedHealth(FeedHealth health) {
        if (health == null || health.source() == null || health.emittedAt() == null) {
            return;
        }
        feedHealth.compute(health.source(), (source, current) ->
                current == null || !health.emittedAt().isBefore(current.emittedAt()) ? health : current);
    }

    /** Removes positions older than {@code vesselExpiry} and identities older than {@code staticExpiry}. */
    @Scheduled(fixedDelay = 30_000)
    public void evictExpired() {
        Instant now = dataTime();
        Instant positionCutoff = now.minus(properties.vesselExpiry());
        Instant staticCutoff = now.minus(properties.staticExpiry());
        latestPositions.values().removeIf(p -> p.time().isBefore(positionCutoff));
        staticByMmsi.values().removeIf(s -> s.reportedAt().isBefore(staticCutoff));
    }

    /** Builds the current snapshot of all non-expired vessels and live feed statuses. */
    public LiveSnapshot snapshot() {
        Instant now = clock.instant();
        Instant cutoff = dataTime().minus(properties.vesselExpiry());
        List<LiveVesselView> vessels = latestPositions.values().stream()
                .filter(p -> !p.time().isBefore(cutoff))
                .sorted(Comparator.comparing(TrackPoint::mmsi))
                .map(p -> toLiveVessel(p, staticByMmsi.get(p.mmsi())))
                .toList();
        return new LiveSnapshot(now, feeds(now), vessels);
    }

    /** Current state of every reported feed, with {@code stale} computed against {@code now}. */
    public List<FeedHealthView> feeds() {
        return feeds(clock.instant());
    }

    private List<FeedHealthView> feeds(Instant now) {
        Duration staleAfter = properties.feedStaleAfter();
        return feedHealth.values().stream()
                .sorted(Comparator.comparing(h -> h.source().name()))
                .map(h -> {
                    boolean heartbeatFresh = Duration.between(h.emittedAt(), now).compareTo(staleAfter) <= 0;
                    boolean messageFresh = h.lastMessageAt() != null
                            && Duration.between(h.lastMessageAt(), now).compareTo(staleAfter) <= 0;
                    boolean stale = !h.connected() || !heartbeatFresh || !messageFresh;
                    return new FeedHealthView(h.source(), h.connected(), stale, h.emittedAt(),
                            h.lastMessageAt(), h.messagesInInterval(), h.positionsInInterval());
                })
                .toList();
    }

    /** The time vessel ages are measured against: the wall clock live, the newest replayed position in replay. */
    private Instant dataTime() {
        return properties.replay() ? newestPosition.get() : clock.instant();
    }

    public TrackPoint latestPosition(String mmsi) {
        return latestPositions.get(mmsi);
    }

    public MergedStatic staticData(String mmsi) {
        return staticByMmsi.get(mmsi);
    }

    private static LiveVesselView toLiveVessel(TrackPoint p, MergedStatic s) {
        MmsiFlag.Flag flag = MmsiFlag.resolve(p.mmsi());
        return new LiveVesselView(
                p.mmsi(),
                p.time(),
                p.latitude(),
                p.longitude(),
                p.sogKnots(),
                p.cogDegrees(),
                p.headingDegrees(),
                p.navStatus(),
                p.source(),
                p.h3Cell(),
                s == null ? null : s.name(),
                s == null ? null : s.imo(),
                s == null ? null : s.callSign(),
                flag == null ? null : flag.code(),
                flag == null ? null : flag.country(),
                s == null ? null : s.shipType(),
                s == null ? null : s.lengthMeters(),
                s == null ? null : s.beamMeters(),
                s == null ? null : s.draughtMeters(),
                s == null ? null : s.destination(),
                s == null ? null : s.eta()
        );
    }

    private static MergedStatic mergeStatic(MergedStatic current, VesselStatic r) {
        String name = cleanText(r.vesselName());
        String imo = cleanText(r.imoNumber());
        String callSign = cleanText(r.callSign());
        String destination = cleanText(r.destination());
        if (current == null) {
            return new MergedStatic(imo, name, callSign, r.shipType(), r.lengthMeters(), r.beamMeters(),
                    r.draughtMeters(), destination, r.eta(), r.source(), r.reportedAt());
        }
        if (!r.reportedAt().isBefore(current.reportedAt())) {
            return new MergedStatic(
                    imo != null ? imo : current.imo(),
                    name != null ? name : current.name(),
                    callSign != null ? callSign : current.callSign(),
                    r.shipType() != null ? r.shipType() : current.shipType(),
                    r.lengthMeters() != null ? r.lengthMeters() : current.lengthMeters(),
                    r.beamMeters() != null ? r.beamMeters() : current.beamMeters(),
                    r.draughtMeters() != null ? r.draughtMeters() : current.draughtMeters(),
                    destination != null ? destination : current.destination(),
                    r.eta() != null ? r.eta() : current.eta(),
                    r.source(),
                    r.reportedAt()
            );
        }
        return new MergedStatic(
                current.imo() != null ? current.imo() : imo,
                current.name() != null ? current.name() : name,
                current.callSign() != null ? current.callSign() : callSign,
                current.shipType() != null ? current.shipType() : r.shipType(),
                current.lengthMeters() != null ? current.lengthMeters() : r.lengthMeters(),
                current.beamMeters() != null ? current.beamMeters() : r.beamMeters(),
                current.draughtMeters() != null ? current.draughtMeters() : r.draughtMeters(),
                current.destination() != null ? current.destination() : destination,
                current.eta() != null ? current.eta() : r.eta(),
                current.source(),
                current.reportedAt()
        );
    }

    private static String cleanText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.replace("\u0000", "").trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static boolean validCoordinates(double lat, double lon) {
        return Double.isFinite(lat) && Double.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180;
    }
}
