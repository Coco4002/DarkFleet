package com.darkfleet.intelligence.read;

import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/** What the read API returns. Detections are observations for an analyst, never findings. */
public final class Views {

    private Views() {
    }

    /** A vessel in a search result. */
    public record VesselSummary(
            String mmsi, String imo, String name, String callSign, Integer shipType, Integer lengthMeters,
            String destination, Instant staticReportedAt, boolean sanctioned, boolean listed, boolean watchlisted) {
    }

    /** Latest identity and voyage data as broadcast. */
    public record VesselData(
            String mmsi, String imo, String name, String callSign, Integer shipType, Integer lengthMeters,
            Integer beamMeters, Double draughtMeters, String destination, Instant eta, String source,
            Instant reportedAt) {
    }

    /** Everything an analyst needs about one vessel. {@code data} is null when no static data arrived yet. */
    public record VesselProfile(
            String mmsi, VesselData data, TrackPointView lastPosition, List<IdentityView> identities,
            List<String> otherMmsisWithSameImo, List<ListingView> listings, List<WatchlistView> watchlist,
            List<DetectionView> recentDetections, List<CaseSummary> cases) {
    }

    /** One identity a vessel broadcast on one feed, with the period it was seen. */
    public record IdentityView(
            String source, String imo, String name, String callSign, Integer shipType, Integer lengthMeters,
            Integer beamMeters, Instant firstSeen, Instant lastSeen) {
    }

    /** An entry on OpenSanctions' maritime list matching the vessel by IMO number or MMSI. */
    public record ListingView(
            String id, String name, String imo, List<String> mmsi, List<String> topics, String flag,
            List<String> datasets, String url) {
    }

    public record WatchlistView(
            String id, String mmsi, String imo, String reason, boolean active, String proposalId, String addedBy,
            Instant addedAt, String removedBy, Instant removedAt) {
    }

    public record TrackPointView(
            Instant time, double latitude, double longitude, Double sogKnots, Double cogDegrees,
            Double headingDegrees, Integer navStatus, String source) {
    }

    /** A vessel's identity, size and thinned points within one hour, for the map's time slider. */
    public record VesselHistoryTrack(
            String mmsi, String name, Integer shipType, Integer lengthMeters, Integer beamMeters,
            List<TrackPointView> points) {
    }

    /** Coarse all-ships history for one hour (1 point per 5-minute bucket per vessel). */
    public record HistoryHourView(
            Instant hourStart, Instant hourEnd, List<VesselHistoryTrack> vessels) {
    }

    public record DetectionView(
            String detectionId, String type, String status, int version, String mmsi, String otherMmsi,
            Instant startedAt, Instant endedAt, Double latitude, Double longitude, String h3Cell,
            int rulesVersion, JsonNode evidence, Instant updatedAt) {
    }

    public record DetectionDetail(DetectionView detection, List<AssessmentView> assessments, List<String> caseIds) {
    }

    public record AssessmentView(
            String id, String detectionId, String category, String rationale, String assessedBy, Instant createdAt) {
    }

    public record CaseSummary(String id, String title, String mmsi, String status, Instant openedAt, Instant closedAt) {
    }

    public record CoverageView(
            String h3Cell, Instant hourStart, JsonNode positionsBySource, int vessels, int stoppedVessels,
            JsonNode windowPositionsBySource, int windowMaxStoppedVessels) {
    }
}
