package com.darkfleet.common.kafka;

public final class DarkFleetTopics {

    /**
     * Live normalized AIS messages (position + static type 5) from the live adapters, keyed by MMSI.
     */
    public static final String AIS_RAW = "ais.raw";

    /**
     * Historical AIS replayed from recorded files, keyed by MMSI. Kept apart from {@link #AIS_RAW} so
     * replayed data can never mix with live state; only a stack started in replay mode reads it.
     */
    public static final String AIS_REPLAY = "ais.replay";

    /**
     * Heartbeats of the live feed adapters ({@code FeedHealth}), keyed by source.
     */
    public static final String FEED_HEALTH = "feed.health";

    /**
     * Reconstructed vessel track updates and derived vessel state, partitioned by MMSI.
     */
    public static final String AIS_TRACKS = "ais.tracks";

    /**
     * Slow-moving cargo/tanker reports away from ports/anchorages, re-keyed by grid cell
     * so two vessels on different MMSI partitions can be compared on the same cell partition.
     */
    public static final String AIS_ENCOUNTER_CANDIDATES = "ais.candidates.encounter";

    /**
     * Candidate anomaly events (unexplained gap, loitering, potential encounter, etc.).
     */
    public static final String AIS_DETECTIONS = "ais.detections";

    /**
     * Coverage statistics per grid cell and hour bucket by source.
     */
    public static final String AIS_COVERAGE = "ais.coverage";

    /**
     * Approved detection rule updates broadcast from Vessel Intelligence to Track & Detection.
     */
    public static final String DARKFLEET_RULES = "darkfleet.rules";

    private DarkFleetTopics() {
    }
}
