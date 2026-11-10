package com.darkfleet.common.kafka;

public final class DarkFleetTopics {

    /**
     * Raw normalized AIS messages (position + static type 5), partitioned by MMSI.
     */
    public static final String AIS_RAW = "ais.raw";

    /**
     * Reconstructed vessel track updates and derived vessel state, partitioned by MMSI.
     */
    public static final String AIS_TRACKS = "ais.tracks";

    /**
     * Slow-moving cargo/tanker reports away from ports/anchorages, re-keyed by H3 grid cell
     * so two vessels on different MMSI partitions can be compared on the same cell partition.
     */
    public static final String AIS_ENCOUNTER_CANDIDATES = "ais.candidates.encounter";

    /**
     * Candidate anomaly events (unexplained gap, loitering, potential encounter, etc.).
     */
    public static final String AIS_DETECTIONS = "ais.detections";

    /**
     * Coverage statistics per H3 grid cell and hour bucket by source.
     */
    public static final String AIS_COVERAGE = "ais.coverage";

    /**
     * Approved detection rule updates broadcast from Vessel Intelligence to Track & Detection.
     */
    public static final String DARKFLEET_RULES = "darkfleet.rules";

    private DarkFleetTopics() {
    }
}
