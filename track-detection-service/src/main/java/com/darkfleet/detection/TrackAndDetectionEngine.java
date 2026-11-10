package com.darkfleet.detection;

import com.darkfleet.common.model.AisMessageType;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.ConfidenceLevel;
import com.darkfleet.common.model.CorridorRegion;
import com.darkfleet.common.model.CoverageCellHour;
import com.darkfleet.common.model.DetectionEvent;
import com.darkfleet.common.model.DetectionRuleConfig;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.common.model.EncounterCandidate;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStaticReport;
import com.darkfleet.common.model.VesselTrack;
import com.darkfleet.common.spatial.SpatialEngine;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stateful streaming engine owned by Track & Detection.
 * Reconstructs vessel tracks, maintains cell/hour coverage, executes cross-source checks,
 * re-keys slow-moving cargo/tanker ships by H3 cell for encounter detection, and emits neutral candidate events.
 */
@Service
public class TrackAndDetectionEngine {

    private static final int MAX_TRAIL_POINTS = 250;

    private final CoverageTracker coverageTracker;
    private final AtomicReference<DetectionRuleConfig> activeRules =
            new AtomicReference<>(DetectionRuleConfig.defaults());

    private final Set<String> processedDedupKeys = ConcurrentHashMap.newKeySet();
    private final Map<String, MutableVesselState> vesselStates = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> imoToMmsis = new ConcurrentHashMap<>();

    /**
     * Co-partitioned H3 cell state store for slow-moving cargo/tanker vessels away from ports/anchorages.
     * Key: h3Cell -> (mmsi -> latest EncounterCandidate)
     */
    private final Map<String, Map<String, EncounterCandidate>> cellCandidates = new ConcurrentHashMap<>();

    /**
     * Pairwise encounter duration tracker. Key: sorted "mmsiA:mmsiB"
     */
    private final Map<String, PairEncounterState> pairEncounters = new ConcurrentHashMap<>();

    /**
     * Recent encounter lookup by MMSI for post-encounter draught change correlation.
     */
    private final Map<String, RecentEncounterRef> recentEncounterByMmsi = new ConcurrentHashMap<>();

    private final List<DetectionEvent> detections = new CopyOnWriteArrayList<>();
    private final Set<String> emittedAlertFingerprints = ConcurrentHashMap.newKeySet();

    public record ProcessingOutcome(
            VesselTrack updatedTrack,
            CoverageCellHour updatedCoverage,
            List<EncounterCandidate> emittedEncounterCandidates,
            List<DetectionEvent> newDetections
    ) {
    }

    public TrackAndDetectionEngine(CoverageTracker coverageTracker) {
        this.coverageTracker = coverageTracker;
    }

    public DetectionRuleConfig getActiveRules() {
        return activeRules.get();
    }

    public void updateRules(DetectionRuleConfig newRules) {
        if (newRules != null) {
            activeRules.set(newRules);
        }
    }

    /**
     * Processes a single normalized AIS message idempotently and returns all derived streaming outputs.
     */
    public synchronized ProcessingOutcome processMessage(NormalizedAisMessage message) {
        if (message == null || !processedDedupKeys.add(message.dedupKey())) {
            VesselTrack existing = getTrack(message != null ? message.mmsi() : "").orElse(null);
            return new ProcessingOutcome(existing, null, List.of(), List.of());
        }

        MutableVesselState state = vesselStates.computeIfAbsent(
                message.mmsi(),
                MutableVesselState::new
        );
        if (message.historical()) {
            state.historical = true;
        }

        List<DetectionEvent> emittedDetections = new ArrayList<>();
        List<EncounterCandidate> encounterCandidates = new ArrayList<>();
        CoverageCellHour updatedCoverage = null;

        if (message.messageType() == AisMessageType.STATIC_VOYAGE) {
            handleStaticMessage(message, state, emittedDetections);
        } else if (message.messageType() == AisMessageType.POSITION_REPORT && message.hasValidCoordinates()) {
            if (message.imoNumber() != null || message.vesselName() != null
                    || message.shipType() != null || message.draughtMeters() != null) {
                handleStaticFieldsFromPosition(message, state, emittedDetections);
            }
            updatedCoverage = handlePositionMessage(message, state, encounterCandidates, emittedDetections);
        }

        for (EncounterCandidate candidate : encounterCandidates) {
            emittedDetections.addAll(processEncounterCandidate(candidate));
        }

        state.updatedAt = message.eventTime();
        VesselTrack snapshot = state.toSnapshot();
        return new ProcessingOutcome(snapshot, updatedCoverage, encounterCandidates, emittedDetections);
    }

    /**
     * Evaluates a slow-moving cargo/tanker candidate re-keyed by H3 grid cell against other vessels in the same cell.
     */
    public synchronized List<DetectionEvent> processEncounterCandidate(EncounterCandidate candidate) {
        if (candidate == null) {
            return List.of();
        }
        if (NormalizedAisMessage.isTugOrPilotType(candidate.shipType())
                || !NormalizedAisMessage.isCargoOrTankerType(candidate.shipType())) {
            return List.of();
        }
        if (SpatialEngine.isNearPortOrAnchorage(candidate.latitude(), candidate.longitude())) {
            return List.of();
        }

        DetectionRuleConfig rules = activeRules.get();
        Map<String, EncounterCandidate> inCell = cellCandidates.computeIfAbsent(
                candidate.h3Cell(),
                k -> new ConcurrentHashMap<>()
        );
        inCell.put(candidate.mmsi(), candidate);

        List<DetectionEvent> events = new ArrayList<>();
        for (Map.Entry<String, EncounterCandidate> entry : inCell.entrySet()) {
            String otherMmsi = entry.getKey();
            if (otherMmsi.equals(candidate.mmsi())) {
                continue;
            }
            EncounterCandidate other = entry.getValue();
            long timeSkewMinutes = Math.abs(Duration.between(candidate.timestamp(), other.timestamp()).toMinutes());
            if (timeSkewMinutes > 20) {
                continue;
            }

            double distanceMeters = SpatialEngine.distanceMeters(
                    candidate.latitude(), candidate.longitude(),
                    other.latitude(), other.longitude()
            );
            String pairKey = pairKey(candidate.mmsi(), otherMmsi);

            if (distanceMeters <= rules.encounterMaxDistanceMeters()
                    && candidate.sogKnots() <= rules.encounterMaxSpeedKnots()
                    && other.sogKnots() <= rules.encounterMaxSpeedKnots()) {

                PairEncounterState pairState = pairEncounters.computeIfAbsent(
                        pairKey,
                        k -> new PairEncounterState(candidate.timestamp(), distanceMeters)
                );
                pairState.lastTogetherAt = candidate.timestamp().isAfter(other.timestamp())
                        ? candidate.timestamp()
                        : other.timestamp();
                pairState.minDistanceMeters = Math.min(pairState.minDistanceMeters, distanceMeters);

                long durationMinutes = Duration.between(pairState.firstTogetherAt, pairState.lastTogetherAt).toMinutes();
                String fingerprint = "ENCOUNTER:" + pairKey + ":" + pairState.firstTogetherAt.getEpochSecond();

                if (durationMinutes >= rules.encounterMinDurationMinutes()
                        && emittedAlertFingerprints.add(fingerprint)) {

                    Map<String, Object> evidence = new LinkedHashMap<>();
                    evidence.put("primaryMmsi", candidate.mmsi());
                    evidence.put("primaryName", candidate.vesselName() != null ? candidate.vesselName() : candidate.mmsi());
                    evidence.put("primaryShipType", candidate.shipType());
                    evidence.put("secondaryMmsi", other.mmsi());
                    evidence.put("secondaryName", other.vesselName() != null ? other.vesselName() : other.mmsi());
                    evidence.put("secondaryShipType", other.shipType());
                    evidence.put("minDistanceMeters", Math.round(pairState.minDistanceMeters));
                    evidence.put("primarySpeedKnots", candidate.sogKnots());
                    evidence.put("secondarySpeedKnots", other.sogKnots());
                    evidence.put("portAnchorageExcluded", true);
                    evidence.put("tugsAndPilotsIgnored", true);

                    DetectionEvent encounterEvent = new DetectionEvent(
                            UUID.randomUUID().toString(),
                            DetectionType.POTENTIAL_ENCOUNTER,
                            "Potential Ship-to-Ship Encounter: "
                                    + displayName(candidate.vesselName(), candidate.mmsi())
                                    + " & " + displayName(other.vesselName(), other.mmsi()),
                            String.format(
                                    "Two cargo/tanker vessels (%s and %s) maintained close proximity (%.0f m apart, <= %.1f kn) for %d minutes outside port and anchorage zones.",
                                    displayName(candidate.vesselName(), candidate.mmsi()),
                                    displayName(other.vesselName(), other.mmsi()),
                                    pairState.minDistanceMeters,
                                    Math.max(candidate.sogKnots(), other.sogKnots()),
                                    durationMinutes
                            ),
                            candidate.mmsi(),
                            other.mmsi(),
                            candidate.vesselName(),
                            candidate.imoNumber(),
                            candidate.shipType(),
                            candidate.latitude(),
                            candidate.longitude(),
                            candidate.h3Cell(),
                            candidate.corridor(),
                            pairState.firstTogetherAt,
                            pairState.lastTogetherAt,
                            durationMinutes,
                            ConfidenceLevel.HIGH,
                            0.84,
                            coverageTracker.getCellCoverageScore(candidate.h3Cell(), candidate.timestamp()),
                            true,
                            true,
                            null,
                            candidate.historical() || other.historical(),
                            evidence
                    );

                    recordDetection(encounterEvent);
                    events.add(encounterEvent);

                    recentEncounterByMmsi.put(candidate.mmsi(), new RecentEncounterRef(
                            encounterEvent.eventId(), other.mmsi(), pairState.lastTogetherAt
                    ));
                    recentEncounterByMmsi.put(other.mmsi(), new RecentEncounterRef(
                            encounterEvent.eventId(), candidate.mmsi(), pairState.lastTogetherAt
                    ));
                }
            } else {
                pairEncounters.remove(pairKey);
            }
        }
        return events;
    }

    private void handleStaticMessage(
            NormalizedAisMessage msg,
            MutableVesselState state,
            List<DetectionEvent> emittedDetections
    ) {
        appendStaticReportAndCheckAnomalies(
                msg.mmsi(),
                msg.imoNumber(),
                msg.vesselName(),
                msg.callSign(),
                msg.shipType(),
                msg.draughtMeters(),
                msg.destination(),
                msg.source(),
                msg.eventTime(),
                msg.historical(),
                state,
                emittedDetections
        );
    }

    private void handleStaticFieldsFromPosition(
            NormalizedAisMessage msg,
            MutableVesselState state,
            List<DetectionEvent> emittedDetections
    ) {
        boolean isNewStaticInfo = (msg.imoNumber() != null && !msg.imoNumber().equals(state.latestImo))
                || (msg.vesselName() != null && !msg.vesselName().equals(state.latestVesselName))
                || (msg.shipType() != null && !msg.shipType().equals(state.shipType))
                || (msg.draughtMeters() != null && !msg.draughtMeters().equals(state.currentDraughtMeters));

        if (isNewStaticInfo) {
            appendStaticReportAndCheckAnomalies(
                    msg.mmsi(),
                    msg.imoNumber(),
                    msg.vesselName(),
                    msg.callSign(),
                    msg.shipType(),
                    msg.draughtMeters(),
                    msg.destination(),
                    msg.source(),
                    msg.eventTime(),
                    msg.historical(),
                    state,
                    emittedDetections
            );
        }
    }

    private void appendStaticReportAndCheckAnomalies(
            String mmsi,
            String imo,
            String name,
            String callSign,
            Integer shipType,
            Double draught,
            String destination,
            AisSource source,
            Instant eventTime,
            boolean historical,
            MutableVesselState state,
            List<DetectionEvent> emittedDetections
    ) {
        DetectionRuleConfig rules = activeRules.get();

        // Check identity inconsistency BEFORE updating latest values (never overwrite history!)
        boolean imoConflict = imo != null && state.latestImo != null && !imo.equals(state.latestImo);
        boolean nameConflict = name != null && state.latestVesselName != null
                && !name.equalsIgnoreCase(state.latestVesselName);

        if (imo != null) {
            Set<String> mmsisForImo = imoToMmsis.computeIfAbsent(imo, k -> ConcurrentHashMap.newKeySet());
            mmsisForImo.add(mmsi);
            if (mmsisForImo.size() > 1) {
                imoConflict = true;
            }
        }

        if (imoConflict || nameConflict) {
            state.hasIdentityConflict = true;
            String fingerprint = "ID_CONFLICT:" + mmsi + ":" + imo + ":" + name;
            if (emittedAlertFingerprints.add(fingerprint)) {
                double lat = state.latestPosition != null ? state.latestPosition.latitude() : 59.80;
                double lon = state.latestPosition != null ? state.latestPosition.longitude() : 25.40;
                String cell = state.currentH3Cell != null ? state.currentH3Cell : SpatialEngine.toH3Cell(lat, lon);

                Map<String, Object> evidence = new LinkedHashMap<>();
                evidence.put("previousImo", state.latestImo);
                evidence.put("newImo", imo);
                evidence.put("previousName", state.latestVesselName);
                evidence.put("newName", name);
                evidence.put("staticReportsKeptInHistory", state.staticHistory.size() + 1);

                DetectionEvent conflictEvent = new DetectionEvent(
                        UUID.randomUUID().toString(),
                        DetectionType.IDENTITY_INCONSISTENCY,
                        "Identity Inconsistency Signal: MMSI " + mmsi,
                        String.format(
                                "MMSI %s broadcast conflicting static identity attributes (previous: name='%s', IMO='%s'; new: name='%s', IMO='%s'). Both reports preserved in history.",
                                mmsi, state.latestVesselName, state.latestImo, name, imo
                        ),
                        mmsi,
                        null,
                        name != null ? name : state.latestVesselName,
                        imo != null ? imo : state.latestImo,
                        shipType != null ? shipType : state.shipType,
                        lat,
                        lon,
                        cell,
                        state.corridor,
                        eventTime,
                        eventTime,
                        0L,
                        ConfidenceLevel.LOW,
                        0.40,
                        coverageTracker.getCellCoverageScore(cell, eventTime),
                        true,
                        false,
                        null,
                        historical,
                        evidence
                );
                recordDetection(conflictEvent);
                emittedDetections.add(conflictEvent);
            }
        }

        // Check draught change after a potential ship-to-ship encounter
        if (draught != null && state.currentDraughtMeters != null) {
            double delta = draught - state.currentDraughtMeters;
            if (Math.abs(delta) >= rules.draughtChangeMinDeltaMeters()) {
                RecentEncounterRef recentEncounter = recentEncounterByMmsi.get(mmsi);
                if (recentEncounter != null
                        && Duration.between(recentEncounter.encounterEndTime(), eventTime).toHours() <= 8) {
                    String fingerprint = "DRAUGHT:" + mmsi + ":" + eventTime.getEpochSecond();
                    if (emittedAlertFingerprints.add(fingerprint)) {
                        double lat = state.latestPosition != null ? state.latestPosition.latitude() : 56.95;
                        double lon = state.latestPosition != null ? state.latestPosition.longitude() : 11.45;
                        String cell = state.currentH3Cell != null ? state.currentH3Cell : SpatialEngine.toH3Cell(lat, lon);

                        Map<String, Object> evidence = new LinkedHashMap<>();
                        evidence.put("previousDraughtMeters", state.currentDraughtMeters);
                        evidence.put("newDraughtMeters", draught);
                        evidence.put("draughtDeltaMeters", Math.round(delta * 100.0) / 100.0);
                        evidence.put("linkedEncounterEventId", recentEncounter.encounterEventId());
                        evidence.put("partnerMmsi", recentEncounter.partnerMmsi());

                        DetectionEvent draughtEvent = new DetectionEvent(
                                UUID.randomUUID().toString(),
                                DetectionType.DRAUGHT_CHANGE,
                                "Post-Encounter Draught Change: " + displayName(name != null ? name : state.latestVesselName, mmsi),
                                String.format(
                                        "Vessel %s reported a static draught change from %.1f m to %.1f m (%+.1f m) following a potential ship-to-ship encounter with MMSI %s.",
                                        displayName(name != null ? name : state.latestVesselName, mmsi),
                                        state.currentDraughtMeters,
                                        draught,
                                        delta,
                                        recentEncounter.partnerMmsi()
                                ),
                                mmsi,
                                recentEncounter.partnerMmsi(),
                                name != null ? name : state.latestVesselName,
                                imo != null ? imo : state.latestImo,
                                shipType != null ? shipType : state.shipType,
                                lat,
                                lon,
                                cell,
                                state.corridor,
                                recentEncounter.encounterEndTime(),
                                eventTime,
                                Duration.between(recentEncounter.encounterEndTime(), eventTime).toMinutes(),
                                ConfidenceLevel.HIGH,
                                0.91,
                                coverageTracker.getCellCoverageScore(cell, eventTime),
                                true,
                                true,
                                Math.round(delta * 100.0) / 100.0,
                                historical,
                                evidence
                        );
                        recordDetection(draughtEvent);
                        emittedDetections.add(draughtEvent);
                    }
                }
            }
        }

        VesselStaticReport report = new VesselStaticReport(
                mmsi, imo, name, callSign, shipType, draught, destination, source, eventTime, historical
        );
        state.staticHistory.add(report);

        if (imo != null) {
            state.latestImo = imo;
        }
        if (name != null) {
            state.latestVesselName = name;
        }
        if (callSign != null) {
            state.latestCallSign = callSign;
        }
        if (shipType != null) {
            state.shipType = shipType;
        }
        if (draught != null) {
            state.previousDraughtMeters = state.currentDraughtMeters;
            state.currentDraughtMeters = draught;
        }
        if (destination != null) {
            state.destination = destination;
        }
    }

    private CoverageCellHour handlePositionMessage(
            NormalizedAisMessage msg,
            MutableVesselState state,
            List<EncounterCandidate> encounterCandidates,
            List<DetectionEvent> emittedDetections
    ) {
        DetectionRuleConfig rules = activeRules.get();
        double lat = msg.latitude();
        double lon = msg.longitude();
        double sog = msg.sogKnots() != null ? msg.sogKnots() : 0.0;
        double cog = msg.cogDegrees() != null ? msg.cogDegrees() : 0.0;
        String h3Cell = SpatialEngine.toH3Cell(lat, lon);
        CorridorRegion corridor = CorridorRegion.classify(lat, lon);

        // 1. Update cell/hour coverage map
        CoverageCellHour updatedCoverage = coverageTracker.recordPosition(
                h3Cell, lat, lon, msg.eventTime(), msg.source(), msg.mmsi()
        );

        TrackPoint currentPoint = new TrackPoint(
                msg.eventTime(),
                lat,
                lon,
                sog,
                cog,
                msg.headingDegrees(),
                msg.navStatus(),
                msg.source(),
                h3Cell,
                msg.historical()
        );

        // 2. Check for AIS gap on this feed and perform Cross-Source Check against other feeds
        TrackPoint prevOnSameSource = state.lastPointBySource.get(msg.source());
        if (prevOnSameSource != null && msg.eventTime().isAfter(prevOnSameSource.timestamp())) {
            long gapMinutes = Duration.between(prevOnSameSource.timestamp(), msg.eventTime()).toMinutes();
            if (gapMinutes >= rules.minGapMinutes() && !NormalizedAisMessage.isTugOrPilotType(state.shipType)) {
                boolean seenByOtherFeed = state.trail.stream()
                        .anyMatch(tp -> tp.source() != msg.source()
                                && tp.timestamp().isAfter(prevOnSameSource.timestamp().plusSeconds(60))
                                && tp.timestamp().isBefore(msg.eventTime().minusSeconds(60)));

                double cellCoverageScore = coverageTracker.getCellCoverageScore(
                        prevOnSameSource.h3Cell(), prevOnSameSource.timestamp()
                );

                double gapDistanceNm = SpatialEngine.distanceNauticalMiles(
                        prevOnSameSource.latitude(), prevOnSameSource.longitude(),
                        lat, lon
                );

                Map<String, Object> evidence = new LinkedHashMap<>();
                evidence.put("reportingSourceWithGap", msg.source().name());
                evidence.put("gapDurationMinutes", gapMinutes);
                evidence.put("distanceTraveledDuringGapNm", Math.round(gapDistanceNm * 100.0) / 100.0);
                evidence.put("lastCellBeforeGap", prevOnSameSource.h3Cell());
                evidence.put("cellCoverageScore", cellCoverageScore);
                evidence.put("seenBySecondaryFeedDuringGap", seenByOtherFeed);

                String gapFingerprint = "GAP:" + msg.mmsi() + ":" + prevOnSameSource.timestamp().getEpochSecond();
                if (emittedAlertFingerprints.add(gapFingerprint)) {
                    if (seenByOtherFeed || cellCoverageScore < rules.minCoverageScoreForGap()) {
                        String reason = seenByOtherFeed
                                ? "Vessel was lost on " + msg.source().name() + " for " + gapMinutes
                                  + " min but continuously tracked by secondary feed; classified as cross-source coverage gap."
                                : "Vessel had a " + gapMinutes + " min gap in cell " + prevOnSameSource.h3Cell()
                                  + " with low measured coverage (" + cellCoverageScore + " < " + rules.minCoverageScoreForGap()
                                  + "); suppressed.";

                        DetectionEvent suppressedGap = new DetectionEvent(
                                UUID.randomUUID().toString(),
                                DetectionType.COVERAGE_GAP_SUPPRESSED,
                                "Cross-Source / Low-Coverage Gap Suppressed: " + displayName(state.latestVesselName, msg.mmsi()),
                                reason,
                                msg.mmsi(),
                                null,
                                state.latestVesselName,
                                state.latestImo,
                                state.shipType,
                                prevOnSameSource.latitude(),
                                prevOnSameSource.longitude(),
                                prevOnSameSource.h3Cell(),
                                corridor,
                                prevOnSameSource.timestamp(),
                                msg.eventTime(),
                                gapMinutes,
                                ConfidenceLevel.LOW,
                                0.15,
                                cellCoverageScore,
                                true,
                                seenByOtherFeed,
                                null,
                                msg.historical(),
                                evidence
                        );
                        recordDetection(suppressedGap);
                        emittedDetections.add(suppressedGap);
                    } else {
                        DetectionEvent unexplainedGap = new DetectionEvent(
                                UUID.randomUUID().toString(),
                                DetectionType.UNEXPLAINED_GAP,
                                "Unexplained AIS Gap: " + displayName(state.latestVesselName, msg.mmsi()),
                                String.format(
                                        "Vessel %s had an unexplained AIS transmission gap of %d minutes (%.1f NM) in well-covered cell %s (coverage score %.2f) and was not seen on secondary feeds.",
                                        displayName(state.latestVesselName, msg.mmsi()),
                                        gapMinutes,
                                        gapDistanceNm,
                                        prevOnSameSource.h3Cell(),
                                        cellCoverageScore
                                ),
                                msg.mmsi(),
                                null,
                                state.latestVesselName,
                                state.latestImo,
                                state.shipType,
                                prevOnSameSource.latitude(),
                                prevOnSameSource.longitude(),
                                prevOnSameSource.h3Cell(),
                                corridor,
                                prevOnSameSource.timestamp(),
                                msg.eventTime(),
                                gapMinutes,
                                cellCoverageScore >= 0.70 ? ConfidenceLevel.HIGH : ConfidenceLevel.MEDIUM,
                                cellCoverageScore >= 0.70 ? 0.86 : 0.68,
                                cellCoverageScore,
                                true,
                                false,
                                null,
                                msg.historical(),
                                evidence
                        );
                        recordDetection(unexplainedGap);
                        emittedDetections.add(unexplainedGap);
                    }
                }
            }
        }

        // 3. Check lower-confidence signals: Position on Land & Impossible Speed Jump
        if (SpatialEngine.isPositionOnLand(lat, lon)) {
            String landFp = "LAND:" + msg.mmsi() + ":" + h3Cell;
            if (emittedAlertFingerprints.add(landFp)) {
                DetectionEvent landEvent = new DetectionEvent(
                        UUID.randomUUID().toString(),
                        DetectionType.POSITION_ON_LAND,
                        "Position on Land Signal: " + displayName(state.latestVesselName, msg.mmsi()),
                        String.format(
                                "Reported coordinates (%.4f, %.4f) for %s fall inside inland landmass polygon (possible GNSS spoofing or receiver corruption).",
                                lat, lon, displayName(state.latestVesselName, msg.mmsi())
                        ),
                        msg.mmsi(),
                        null,
                        state.latestVesselName,
                        state.latestImo,
                        state.shipType,
                        lat,
                        lon,
                        h3Cell,
                        corridor,
                        msg.eventTime(),
                        msg.eventTime(),
                        0L,
                        ConfidenceLevel.LOW,
                        0.35,
                        updatedCoverage.coverageScore(),
                        true,
                        false,
                        null,
                        msg.historical(),
                        Map.of("latitude", lat, "longitude", lon, "source", msg.source().name())
                );
                recordDetection(landEvent);
                emittedDetections.add(landEvent);
            }
        }

        if (state.latestPosition != null && msg.eventTime().isAfter(state.latestPosition.timestamp())) {
            long seconds = Duration.between(state.latestPosition.timestamp(), msg.eventTime()).getSeconds();
            if (seconds >= 30) {
                double impliedKnots = SpatialEngine.impliedSpeedKnots(
                        state.latestPosition.latitude(), state.latestPosition.longitude(), state.latestPosition.timestamp(),
                        lat, lon, msg.eventTime()
                );
                if (impliedKnots > rules.impossibleSpeedThresholdKnots()) {
                    String jumpFp = "JUMP:" + msg.mmsi() + ":" + msg.eventTime().getEpochSecond();
                    if (emittedAlertFingerprints.add(jumpFp)) {
                        DetectionEvent jumpEvent = new DetectionEvent(
                                UUID.randomUUID().toString(),
                                DetectionType.IMPOSSIBLE_JUMP,
                                "Impossible Position Jump Signal: " + displayName(state.latestVesselName, msg.mmsi()),
                                String.format(
                                        "Implied speed of %.1f knots between consecutive positions exceeds physical vessel limits (%.1f knots threshold).",
                                        impliedKnots, rules.impossibleSpeedThresholdKnots()
                                ),
                                msg.mmsi(),
                                null,
                                state.latestVesselName,
                                state.latestImo,
                                state.shipType,
                                lat,
                                lon,
                                h3Cell,
                                corridor,
                                state.latestPosition.timestamp(),
                                msg.eventTime(),
                                seconds / 60,
                                ConfidenceLevel.LOW,
                                0.38,
                                updatedCoverage.coverageScore(),
                                true,
                                false,
                                null,
                                msg.historical(),
                                Map.of("impliedSpeedKnots", Math.round(impliedKnots * 10.0) / 10.0)
                        );
                        recordDetection(jumpEvent);
                        emittedDetections.add(jumpEvent);
                    }
                }
            }
        }

        // 4. Offshore Loitering check (only cargo/tankers 70-89, outside ports & anchorages)
        boolean cargoOrTanker = NormalizedAisMessage.isCargoOrTankerType(state.shipType);
        boolean nearPort = SpatialEngine.isNearPortOrAnchorage(lat, lon);

        if (cargoOrTanker && !nearPort && sog <= rules.loiteringMaxSpeedKnots()) {
            if (state.loiteringStartedAt == null) {
                state.loiteringStartedAt = msg.eventTime();
            } else if (msg.eventTime().isAfter(state.loiteringStartedAt)) {
                long loiterMinutes = Duration.between(state.loiteringStartedAt, msg.eventTime()).toMinutes();
                String loiterFp = "LOITER:" + msg.mmsi() + ":" + state.loiteringStartedAt.getEpochSecond();
                if (loiterMinutes >= rules.loiteringMinDurationMinutes()
                        && emittedAlertFingerprints.add(loiterFp)) {
                    DetectionEvent loiterEvent = new DetectionEvent(
                            UUID.randomUUID().toString(),
                            DetectionType.LOITERING,
                            "Offshore Loitering Candidate: " + displayName(state.latestVesselName, msg.mmsi()),
                            String.format(
                                    "Cargo/tanker vessel %s maintained low speed (%.1f knots) offshore outside known ports and anchorages for %d minutes.",
                                    displayName(state.latestVesselName, msg.mmsi()),
                                    sog,
                                    loiterMinutes
                            ),
                            msg.mmsi(),
                            null,
                            state.latestVesselName,
                            state.latestImo,
                            state.shipType,
                            lat,
                            lon,
                            h3Cell,
                            corridor,
                            state.loiteringStartedAt,
                            msg.eventTime(),
                            loiterMinutes,
                            ConfidenceLevel.MEDIUM,
                            0.68,
                            updatedCoverage.coverageScore(),
                            true,
                            false,
                            null,
                            msg.historical(),
                            Map.of(
                                    "sogKnots", sog,
                                    "loiteringDurationMinutes", loiterMinutes,
                                    "portAnchorageExcluded", true
                            )
                    );
                    recordDetection(loiterEvent);
                    emittedDetections.add(loiterEvent);
                }
            }
        } else {
            state.loiteringStartedAt = null;
        }

        // 5. Re-key slow-moving cargo/tankers (AIS 70-89, ignoring tugs/pilots) away from ports/anchorages by H3 cell
        if (cargoOrTanker && !nearPort && sog <= rules.encounterMaxSpeedKnots()) {
            for (String targetCell : SpatialEngine.cellAndNeighbors(lat, lon)) {
                encounterCandidates.add(new EncounterCandidate(
                        targetCell,
                        msg.mmsi(),
                        state.latestImo,
                        state.latestVesselName,
                        state.shipType,
                        lat,
                        lon,
                        sog,
                        cog,
                        state.currentDraughtMeters,
                        corridor,
                        msg.source(),
                        msg.eventTime(),
                        msg.historical()
                ));
            }
        }

        // 6. Append point to reconstructed track state
        state.trail.add(currentPoint);
        state.trail.sort(Comparator.comparing(TrackPoint::timestamp));
        while (state.trail.size() > MAX_TRAIL_POINTS) {
            state.trail.remove(0);
        }
        state.latestPosition = state.trail.get(state.trail.size() - 1);
        state.currentH3Cell = SpatialEngine.toH3Cell(
                state.latestPosition.latitude(),
                state.latestPosition.longitude()
        );
        state.corridor = CorridorRegion.classify(
                state.latestPosition.latitude(),
                state.latestPosition.longitude()
        );
        state.lastSeenBySource.put(msg.source(), msg.eventTime());
        state.lastPointBySource.put(msg.source(), currentPoint);

        return updatedCoverage;
    }

    public List<VesselTrack> listTracks(CorridorRegion corridorFilter) {
        return vesselStates.values().stream()
                .map(MutableVesselState::toSnapshot)
                .filter(t -> corridorFilter == null || t.corridor() == corridorFilter)
                .sorted(Comparator.comparing(
                        VesselTrack::updatedAt,
                        Comparator.nullsLast(Comparator.reverseOrder())
                ))
                .toList();
    }

    public Optional<VesselTrack> getTrack(String mmsi) {
        MutableVesselState state = vesselStates.get(mmsi);
        return state != null ? Optional.of(state.toSnapshot()) : Optional.empty();
    }

    public List<VesselTrack> getTracksInWindow(Instant from, Instant to) {
        List<VesselTrack> results = new ArrayList<>();
        for (MutableVesselState state : vesselStates.values()) {
            List<TrackPoint> filteredTrail = state.trail.stream()
                    .filter(tp -> !tp.timestamp().isBefore(from) && !tp.timestamp().isAfter(to))
                    .toList();
            if (!filteredTrail.isEmpty()) {
                TrackPoint latestInWindow = filteredTrail.get(filteredTrail.size() - 1);
                results.add(new VesselTrack(
                        state.mmsi,
                        state.latestImo,
                        state.latestVesselName,
                        state.latestCallSign,
                        state.shipType,
                        state.currentDraughtMeters,
                        state.previousDraughtMeters,
                        state.destination,
                        CorridorRegion.classify(latestInWindow.latitude(), latestInWindow.longitude()),
                        latestInWindow.h3Cell(),
                        latestInWindow,
                        Map.copyOf(state.lastSeenBySource),
                        filteredTrail,
                        List.copyOf(state.staticHistory),
                        state.hasIdentityConflict,
                        state.historical,
                        latestInWindow.timestamp()
                ));
            }
        }
        return results;
    }

    public List<DetectionEvent> listDetections(
            DetectionType typeFilter,
            String mmsiFilter,
            CorridorRegion corridorFilter,
            boolean includeSuppressed
    ) {
        return detections.stream()
                .filter(d -> includeSuppressed || d.detectionType() != DetectionType.COVERAGE_GAP_SUPPRESSED)
                .filter(d -> typeFilter == null || d.detectionType() == typeFilter)
                .filter(d -> mmsiFilter == null || mmsiFilter.isBlank()
                        || mmsiFilter.equals(d.mmsi()) || mmsiFilter.equals(d.secondaryMmsi()))
                .filter(d -> corridorFilter == null || d.corridor() == corridorFilter)
                .sorted(Comparator.comparing(DetectionEvent::endTime).reversed())
                .toList();
    }

    public List<DetectionEvent> searchEncounters(String mmsiFilter, CorridorRegion corridorFilter) {
        return detections.stream()
                .filter(d -> d.detectionType() == DetectionType.POTENTIAL_ENCOUNTER
                        || d.detectionType() == DetectionType.DRAUGHT_CHANGE)
                .filter(d -> mmsiFilter == null || mmsiFilter.isBlank()
                        || mmsiFilter.equals(d.mmsi()) || mmsiFilter.equals(d.secondaryMmsi()))
                .filter(d -> corridorFilter == null || d.corridor() == corridorFilter)
                .sorted(Comparator.comparing(DetectionEvent::endTime).reversed())
                .toList();
    }

    public synchronized void resetState() {
        processedDedupKeys.clear();
        vesselStates.clear();
        imoToMmsis.clear();
        cellCandidates.clear();
        pairEncounters.clear();
        recentEncounterByMmsi.clear();
        detections.clear();
        emittedAlertFingerprints.clear();
        coverageTracker.clear();
    }

    private void recordDetection(DetectionEvent event) {
        detections.add(event);
    }

    private static String pairKey(String mmsi1, String mmsi2) {
        return mmsi1.compareTo(mmsi2) <= 0 ? mmsi1 + ":" + mmsi2 : mmsi2 + ":" + mmsi1;
    }

    private static String displayName(String name, String mmsi) {
        return (name != null && !name.isBlank()) ? name + " (" + mmsi + ")" : "MMSI " + mmsi;
    }

    private static final class PairEncounterState {
        private final Instant firstTogetherAt;
        private Instant lastTogetherAt;
        private double minDistanceMeters;

        private PairEncounterState(Instant firstTogetherAt, double minDistanceMeters) {
            this.firstTogetherAt = firstTogetherAt;
            this.lastTogetherAt = firstTogetherAt;
            this.minDistanceMeters = minDistanceMeters;
        }
    }

    private record RecentEncounterRef(
            String encounterEventId,
            String partnerMmsi,
            Instant encounterEndTime
    ) {
    }

    private static final class MutableVesselState {
        private final String mmsi;
        private String latestImo;
        private String latestVesselName;
        private String latestCallSign;
        private Integer shipType;
        private Double currentDraughtMeters;
        private Double previousDraughtMeters;
        private String destination;
        private CorridorRegion corridor = CorridorRegion.BALTIC_CORRIDOR;
        private String currentH3Cell;
        private TrackPoint latestPosition;
        private final Map<AisSource, Instant> lastSeenBySource = new EnumMap<>(AisSource.class);
        private final Map<AisSource, TrackPoint> lastPointBySource = new EnumMap<>(AisSource.class);
        private final List<TrackPoint> trail = new ArrayList<>();
        private final List<VesselStaticReport> staticHistory = new ArrayList<>();
        private boolean hasIdentityConflict;
        private boolean historical;
        private Instant loiteringStartedAt;
        private Instant updatedAt = Instant.now();

        private MutableVesselState(String mmsi) {
            this.mmsi = mmsi;
        }

        VesselTrack toSnapshot() {
            return new VesselTrack(
                    mmsi,
                    latestImo,
                    latestVesselName,
                    latestCallSign,
                    shipType,
                    currentDraughtMeters,
                    previousDraughtMeters,
                    destination,
                    corridor,
                    currentH3Cell,
                    latestPosition,
                    Map.copyOf(lastSeenBySource),
                    List.copyOf(trail),
                    List.copyOf(staticHistory),
                    hasIdentityConflict,
                    historical,
                    updatedAt
            );
        }
    }
}
