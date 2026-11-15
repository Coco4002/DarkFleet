package com.darkfleet.detection.vessel;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.detection.DetectionClock;
import com.darkfleet.detection.DetectionProperties;
import com.darkfleet.detection.Lookups;
import com.darkfleet.detection.coverage.CoverageTick;
import com.darkfleet.detection.encounter.EncounterCandidate;
import com.darkfleet.detection.identity.IdentityClaim;
import com.darkfleet.detection.support.Evidence;
import com.darkfleet.detection.support.Geo;
import com.darkfleet.detection.support.Grid;
import com.darkfleet.detection.support.Imo;
import com.darkfleet.detection.vessel.VesselState.Loitering;
import com.darkfleet.detection.vessel.VesselState.Position;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Per-vessel processing, partitioned by MMSI. Merges both live feeds into one track and publishes
 * it thinned, forwards changed static data, and runs the per-vessel detections:
 * <ul>
 *   <li><b>Unexplained gap</b>: a cargo vessel or tanker silent for the gap time, checked by a timer
 *       because a silent vessel sends nothing that could trigger the check. Raised only if a feed that
 *       was receiving the vessel just before it went silent has kept working all that time and covers
 *       both where the vessel was last seen and where it would be by now at its last speed and course.
 *       Never raised in a port or anchorage cell or for a moored vessel. Closed when it reports again.</li>
 *   <li><b>Loitering</b>: a cargo vessel or tanker below loitering speed, not moored and away from
 *       ports and anchorages, for the loitering time. Ends after two reports in a row that aren't slow,
 *       or when no slow report arrives for the episode break.</li>
 *   <li><b>Identity</b>: this MMSI broadcasting two IMO numbers in overlapping periods within the
 *       identity window. The IMO-side check (one IMO, several MMSIs) runs after re-keying by IMO.</li>
 * </ul>
 * Slow cargo vessels and tankers away from ports are also sent on as encounter candidates.
 * Outputs are {@link TrackPoint}, {@link VesselStatic} and {@link Detection} keyed by MMSI,
 * {@link EncounterCandidate} keyed by H3 cell and {@link IdentityClaim} keyed by IMO number.
 */
public class VesselProcessor implements Processor<String, NormalizedAisMessage, String, Object> {

    public static final String STORE = "vessels";
    /** How often an unchanged IMO claim is repeated to the IMO side, so it knows the MMSI is still active. */
    private static final Duration CLAIM_REFRESH = Duration.ofHours(1);
    /** Two reports in a row that aren't slow end a loitering episode. */
    private static final int LOITERING_MISSES = 2;

    private final DetectionProperties properties;
    private final DetectionClock clock;
    private ProcessorContext<String, Object> context;
    private KeyValueStore<String, VesselState> vessels;
    private Lookups lookups;

    public VesselProcessor(DetectionProperties properties, DetectionClock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void init(ProcessorContext<String, Object> context) {
        this.context = context;
        this.vessels = context.getStateStore(STORE);
        this.lookups = new Lookups(context, properties);
        clock.schedule(context, this::check);
    }

    @Override
    public void process(Record<String, NormalizedAisMessage> record) {
        NormalizedAisMessage message = record.value();
        if (message == null
                || message.messageType() == AisMessageType.POSITION_REPORT && !message.hasValidCoordinates()) {
            return;
        }
        String mmsi = message.mmsi();
        VesselState vessel = vessels.get(mmsi);
        if (vessel == null) {
            vessel = new VesselState();
        }
        DetectionRules rules = lookups.rules();
        if (message.messageType() == AisMessageType.POSITION_REPORT) {
            position(mmsi, vessel, message, rules);
        } else {
            staticData(mmsi, vessel, message, rules);
        }
        vessels.put(mmsi, vessel);
    }

    private void position(String mmsi, VesselState vessel, NormalizedAisMessage m, DetectionRules rules) {
        Instant time = m.eventTime();
        vessel.lastSeenBySource.merge(m.source(), time, (a, b) -> a.isAfter(b) ? a : b);
        if (vessel.last != null && !time.isAfter(vessel.last.time())) {
            return; // older, or the same transmission already received through the other feed
        }
        Position p = new Position(time, m.latitude(), m.longitude(), m.sogKnots(), m.cogDegrees(), m.navStatus(),
                m.source(), Grid.cell(m.latitude(), m.longitude()));
        if (vessel.gap != null) {
            closeGap(mmsi, vessel, p);
        }
        vessel.last = p;
        vessel.silenceSettled = false;

        if (trackPointDue(vessel.lastTrackPoint, p)) {
            TrackPoint point = new TrackPoint(mmsi, time, p.latitude(), p.longitude(), p.sogKnots(), p.cogDegrees(),
                    m.headingDegrees(), p.navStatus(), p.source(), p.h3Cell());
            vessel.lastTrackPoint = point;
            forward(mmsi, point, time);
            encounterCandidate(mmsi, vessel, p, rules);
        }
        if (NormalizedAisMessage.isCargoOrTankerType(vessel.shipType)) {
            loitering(mmsi, vessel, p, rules);
        }
    }

    private boolean trackPointDue(TrackPoint previous, Position p) {
        if (previous == null || Duration.between(previous.time(), p.time()).compareTo(properties.trackPointInterval()) >= 0) {
            return true;
        }
        boolean turned = previous.cogDegrees() != null && p.cogDegrees() != null
                && Geo.courseDifference(previous.cogDegrees(), p.cogDegrees()) >= properties.trackTurnDegrees();
        boolean speedChanged = previous.sogKnots() != null && p.sogKnots() != null
                && Math.abs(previous.sogKnots() - p.sogKnots()) >= properties.trackSpeedChangeKnots();
        return turned || speedChanged;
    }

    private void encounterCandidate(String mmsi, VesselState vessel, Position p, DetectionRules rules) {
        if (!NormalizedAisMessage.isCargoOrTankerType(vessel.shipType) || p.sogKnots() == null
                || p.sogKnots() > rules.encounterMaxSpeedKnots() || p.moored()
                || lookups.portOrAnchorage(p.h3Cell(), p.time(), rules)) {
            return;
        }
        for (String cell : Grid.cellsWithin(p.latitude(), p.longitude(), rules.encounterMaxDistanceMeters())) {
            forward(cell, new EncounterCandidate(mmsi, p.time(), p.latitude(), p.longitude(), p.sogKnots(),
                    vessel.draughtMeters, cell.equals(p.h3Cell())), p.time());
        }
    }

    // ---- loitering

    private void loitering(String mmsi, VesselState vessel, Position p, DetectionRules rules) {
        if (p.sogKnots() == null) {
            return;
        }
        boolean inPort = lookups.portOrAnchorage(p.h3Cell(), p.time(), rules);
        boolean slow = p.sogKnots() <= rules.loiteringMaxSpeedKnots() && !p.moored() && !inPort;
        Loitering episode = vessel.loitering;
        if (!slow) {
            if (episode != null && ++episode.misses >= LOITERING_MISSES) {
                endLoitering(mmsi, vessel, inPort || p.moored() ? "in port or moored" : "moving again");
            }
            return;
        }
        if (episode != null && Duration.between(episode.lastSlowAt, p.time()).compareTo(properties.episodeBreak()) > 0) {
            endLoitering(mmsi, vessel, "no slow reports");
            episode = null;
        }
        if (episode == null) {
            episode = new Loitering();
            episode.startedAt = p.time();
            episode.startLatitude = p.latitude();
            episode.startLongitude = p.longitude();
            episode.startCell = p.h3Cell();
            vessel.loitering = episode;
        }
        episode.lastSlowAt = p.time();
        episode.lastLatitude = p.latitude();
        episode.lastLongitude = p.longitude();
        episode.misses = 0;
        if (episode.opened == null
                && Duration.between(episode.startedAt, p.time()).toMinutes() >= rules.loiteringMinMinutes()) {
            episode.opened = new Detection("loitering:" + mmsi + ":" + episode.startedAt.getEpochSecond(),
                    DetectionType.LOITERING, DetectionStatus.OPEN, 1, mmsi, null, episode.startedAt, null,
                    episode.startLatitude, episode.startLongitude, episode.startCell, rules.version(),
                    Evidence.of("shipType", vessel.shipType, "vesselName", vessel.vesselName,
                            "imoNumber", vessel.imoNumber, "draughtMeters", vessel.draughtMeters,
                            "maxSpeedKnots", rules.loiteringMaxSpeedKnots(), "minMinutes", rules.loiteringMinMinutes()));
            forward(mmsi, episode.opened, p.time());
        }
    }

    private void endLoitering(String mmsi, VesselState vessel, String reason) {
        Loitering episode = vessel.loitering;
        vessel.loitering = null;
        if (episode.opened == null) {
            return;
        }
        double km = Geo.distanceMeters(episode.startLatitude, episode.startLongitude,
                episode.lastLatitude, episode.lastLongitude) / 1000;
        forward(mmsi, episode.opened.closed(episode.lastSlowAt, Evidence.of(
                "durationMinutes", Duration.between(episode.startedAt, episode.lastSlowAt).toMinutes(),
                "distanceFromStartKm", Evidence.round(km, 1),
                "closeReason", reason)), episode.lastSlowAt);
    }

    // ---- static data and identity

    private void staticData(String mmsi, VesselState vessel, NormalizedAisMessage m, DetectionRules rules) {
        Instant time = m.eventTime();
        Instant lastFromFeed = vessel.lastStaticAt.get(m.source());
        if (lastFromFeed != null && !time.isAfter(lastFromFeed)) {
            return; // older than what this feed already told us
        }
        vessel.lastStaticAt.put(m.source(), time);
        if (vessel.identityAt == null || !time.isBefore(vessel.identityAt)) { // the other feed may be behind
            vessel.identityAt = time;
            if (m.shipType() != null) {
                vessel.shipType = m.shipType();
            }
            if (m.draughtMeters() != null) {
                vessel.draughtMeters = m.draughtMeters();
            }
            if (m.vesselName() != null) {
                vessel.vesselName = m.vesselName();
            }
            if (m.imoNumber() != null) {
                vessel.imoNumber = m.imoNumber();
            }
        }
        VesselStatic reported = VesselStatic.from(m);
        VesselStatic previous = vessel.publishedStatic.get(m.source());
        if (previous == null || !reported.sameDataAs(previous)
                || Duration.between(previous.reportedAt(), reported.reportedAt()).compareTo(properties.staticRefresh()) >= 0) {
            vessel.publishedStatic.put(m.source(), reported);
            forward(mmsi, reported, reported.reportedAt());
        }
        if (Imo.isValid(m.imoNumber())) {
            imoReported(mmsi, vessel, m, rules);
        }
    }

    private void imoReported(String mmsi, VesselState vessel, NormalizedAisMessage m, DetectionRules rules) {
        String imo = m.imoNumber();
        Instant time = m.eventTime();
        vessel.imoSightings.record(imo, time);
        vessel.imoSightings.forgetBefore(time.minus(Duration.ofHours(rules.identityWindowHours())));
        List<String> conflicting = vessel.imoSightings.conflicting();
        if (conflicting.size() >= 2) {
            vessel.identityConflictSeenAt = later(vessel.identityConflictSeenAt, time);
        }
        if (conflicting.size() >= 2 && vessel.identityConflict == null) {
            Position at = vessel.last;
            vessel.identityConflict = new Detection("identity:mmsi:" + mmsi + ":" + time.getEpochSecond(),
                    DetectionType.IDENTITY_INCONSISTENCY, DetectionStatus.OPEN, 1, mmsi, null, time, null,
                    at == null ? null : at.latitude(), at == null ? null : at.longitude(), at == null ? null : at.h3Cell(),
                    rules.version(), Evidence.of("kind", "one MMSI, several IMO numbers",
                            "imoNumbers", String.join(", ", conflicting), "vesselName", vessel.vesselName));
            forward(mmsi, vessel.identityConflict, time);
        }

        if (!imo.equals(vessel.claimedImo) || vessel.claimedAt == null
                || Duration.between(vessel.claimedAt, time).compareTo(CLAIM_REFRESH) >= 0) {
            vessel.claimedImo = imo;
            vessel.claimedAt = time;
            forward(imo, new IdentityClaim(imo, mmsi, m.vesselName(), time, m.source()), time);
        }
    }

    // ---- timer: silent vessels, ended episodes, forgotten vessels

    private void check(Instant now) {
        lookups.refreshFeeds(now);
        DetectionRules rules = lookups.rules();
        List<KeyValue<String, VesselState>> all = new ArrayList<>();
        try (KeyValueIterator<String, VesselState> iterator = vessels.all()) {
            iterator.forEachRemaining(all::add);
        }
        for (KeyValue<String, VesselState> entry : all) {
            String mmsi = entry.key;
            VesselState vessel = entry.value;
            if (Duration.between(vessel.lastHeard(), now).compareTo(properties.forgetAfter()) > 0) {
                forget(mmsi, vessel, now);
                continue;
            }
            boolean changed = false;
            if (vessel.loitering != null
                    && Duration.between(vessel.loitering.lastSlowAt, now).compareTo(properties.episodeBreak()) > 0) {
                endLoitering(mmsi, vessel, "no slow reports");
                changed = true;
            }
            if (vessel.identityConflict != null) {
                vessel.imoSightings.forgetBefore(now.minus(Duration.ofHours(rules.identityWindowHours())));
                if (vessel.imoSightings.conflicting().size() < 2) {
                    Instant seenAt = vessel.identityConflictSeenAt;
                    forward(mmsi, vessel.identityConflict.closed(seenAt,
                            Evidence.of("closeReason", "no longer conflicting")), seenAt);
                    vessel.identityConflict = null;
                    vessel.identityConflictSeenAt = null;
                }
                changed = true;
            }
            if (vessel.gap == null && !vessel.silenceSettled && vessel.last != null
                    && NormalizedAisMessage.isCargoOrTankerType(vessel.shipType)
                    && Duration.between(vessel.last.time(), now).toMinutes() >= rules.gapMinutes()) {
                checkSilence(mmsi, vessel, now, rules);
                changed |= vessel.gap != null || vessel.silenceSettled;
            }
            if (changed) {
                vessels.put(mmsi, vessel);
            }
        }
    }

    /**
     * Decides about a vessel silent for the gap time. Opens a gap if some feed that was receiving it
     * has been working for the gap time since and covers both its last cell and the cell it would be in
     * by now. Settles the silence without a gap (no further checks until it reports again) when it went
     * silent in a port or anchorage or moored, when where it would be by now is unknown (it was moving
     * but its speed or course wasn't reported), or when no feed covering it saw it disappear inside
     * its coverage. Decides nothing yet while a feed that was receiving it is not working, or has not
     * been working for the gap time.
     */
    private void checkSilence(String mmsi, VesselState vessel, Instant now, DetectionRules rules) {
        Position last = vessel.last;
        double[] expected = expectedPosition(last, now);
        if (last.moored() || lookups.portOrAnchorage(last.h3Cell(), now, rules) || expected == null) {
            vessel.silenceSettled = true;
            return;
        }
        Duration gapTime = Duration.ofMinutes(rules.gapMinutes());
        String expectedCell = Grid.cell(expected[0], expected[1]);
        List<String> watching = new ArrayList<>();
        boolean undecided = false;
        for (AisSource source : AisSource.values()) {
            Instant seen = vessel.lastSeenBySource.get(source);
            if (seen == null || seen.isBefore(last.time().minus(gapTime))) {
                continue; // this feed wasn't receiving the vessel when it went silent
            }
            Instant workingSince = lookups.workingSince(source);
            Instant watchedFrom = workingSince == null || workingSince.isAfter(last.time()) ? workingSince : last.time();
            if (watchedFrom == null || Duration.between(watchedFrom, now).compareTo(gapTime) < 0) {
                undecided = true;
            } else if (lookups.covered(last.h3Cell(), source, now, rules)
                    && lookups.covered(expectedCell, source, now, rules)) {
                watching.add(source.name());
            }
        }
        if (watching.isEmpty()) {
            vessel.silenceSettled = !undecided;
            return;
        }
        vessel.gap = new Detection("gap:" + mmsi + ":" + last.time().getEpochSecond(), DetectionType.UNEXPLAINED_GAP,
                DetectionStatus.OPEN, 1, mmsi, null, last.time(), null, last.latitude(), last.longitude(), last.h3Cell(),
                rules.version(), Evidence.of(
                        "lastSource", last.source().name(), "watchingFeeds", String.join(", ", watching),
                        "lastSpeedKnots", last.sogKnots(), "lastCourseDegrees", last.cogDegrees(),
                        "expectedCell", expectedCell, "shipType", vessel.shipType, "vesselName", vessel.vesselName,
                        "imoNumber", vessel.imoNumber, "draughtMeters", vessel.draughtMeters,
                        "gapMinutes", rules.gapMinutes()));
        forward(mmsi, vessel.gap, now);
    }

    /**
     * Where the vessel would be now at its last speed and course: still where it was if it was
     * stopped, null if it was moving (or its speed is unknown) and its course or speed isn't known.
     */
    private static double[] expectedPosition(Position last, Instant now) {
        if (last.sogKnots() != null && last.sogKnots() < CoverageTick.STOPPED_KNOTS) {
            return new double[]{last.latitude(), last.longitude()};
        }
        if (last.sogKnots() == null || last.cogDegrees() == null) {
            return null;
        }
        double hours = Duration.between(last.time(), now).toSeconds() / 3600.0;
        return Geo.destination(last.latitude(), last.longitude(), last.cogDegrees(),
                last.sogKnots() * hours * Geo.METERS_PER_NAUTICAL_MILE);
    }

    private void closeGap(String mmsi, VesselState vessel, Position reappeared) {
        Position last = vessel.last;
        double km = Geo.distanceMeters(last.latitude(), last.longitude(), reappeared.latitude(), reappeared.longitude()) / 1000;
        double hours = Duration.between(last.time(), reappeared.time()).toSeconds() / 3600.0;
        Map<String, Object> more = Evidence.of(
                "closeReason", "reported again",
                "reappearedSource", reappeared.source().name(),
                "reappearedLatitude", reappeared.latitude(), "reappearedLongitude", reappeared.longitude(),
                "durationMinutes", Duration.between(last.time(), reappeared.time()).toMinutes(),
                "distanceKm", Evidence.round(km, 1),
                "impliedSpeedKnots", Evidence.round(km * 1000 / Geo.METERS_PER_NAUTICAL_MILE / hours, 1));
        forward(mmsi, vessel.gap.closed(reappeared.time(), more), reappeared.time());
        vessel.gap = null;
    }

    private void forget(String mmsi, VesselState vessel, Instant now) {
        String reason = "not heard from for " + properties.forgetAfter().toHours() + " h";
        if (vessel.gap != null) {
            forward(mmsi, vessel.gap.closed(now, Evidence.of("closeReason", reason)), now);
        }
        if (vessel.loitering != null) {
            endLoitering(mmsi, vessel, reason);
        }
        if (vessel.identityConflict != null) {
            forward(mmsi, vessel.identityConflict.closed(now, Evidence.of("closeReason", reason)), now);
        }
        vessels.delete(mmsi);
    }

    private static Instant later(Instant a, Instant b) {
        return a == null || b.isAfter(a) ? b : a;
    }

    private void forward(String key, Object value, Instant time) {
        context.forward(new Record<>(key, value, time.toEpochMilli()));
    }
}
