package com.darkfleet.common.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;
import java.util.Map;

/**
 * Topic names and their settings. Every service declares the topics it reads or writes from here,
 * so the broker never needs auto-create and the settings can't drift between services.
 */
public final class DarkFleetTopics {

    /** Live normalized AIS messages (positions and static data) from the live adapters, keyed by MMSI. */
    public static final String AIS_RAW = "ais.raw";

    /**
     * Historical AIS replayed from recorded files, keyed by MMSI. Kept apart from {@link #AIS_RAW} so
     * replayed data can never mix with live state; only a stack started in replay mode reads it.
     */
    public static final String AIS_REPLAY = "ais.replay";

    /** Heartbeats of the live feed adapters ({@code FeedHealth}), keyed by source. */
    public static final String FEED_HEALTH = "feed.health";

    /** Vessel identity and voyage data ({@code VesselStatic}) when it changes, keyed by MMSI. */
    public static final String VESSEL_STATIC = "vessel.static";

    /** Thinned vessel positions merged across feeds ({@code TrackPoint}), keyed by MMSI. */
    public static final String TRACK_POINTS = "track.points";

    /** Hourly reception statistics per H3 cell ({@code CoverageCell}), keyed by cell. */
    public static final String COVERAGE_CELLS = "coverage.cells";

    /** Detection lifecycle updates ({@code Detection}), keyed by MMSI. */
    public static final String DETECTIONS = "detections";

    /** The approved detection rule set ({@code DetectionRules}), keyed by {@code DetectionRules.KEY}. */
    public static final String RULES_APPROVED = "rules.approved";

    /** Prefix of the output topics a stack in replay mode writes instead of the live ones. */
    public static final String REPLAY_PREFIX = "replay.";

    private static final int PARTITIONS = 6;
    private static final short REPLICAS = 1;

    private DarkFleetTopics() {
    }

    /** The output topic for the given mode: the live name, or the replay copy of it. */
    public static String output(String topic, boolean replay) {
        return replay ? REPLAY_PREFIX + topic : topic;
    }

    /** Topic declaration with this project's settings; accepts replay copies of the output topics too. */
    public static NewTopic newTopic(String name) {
        String base = name.startsWith(REPLAY_PREFIX) ? name.substring(REPLAY_PREFIX.length()) : name;
        return switch (base) {
            case AIS_RAW, AIS_REPLAY -> topic(name, PARTITIONS, delete(Duration.ofHours(48)));
            case FEED_HEALTH -> topic(name, 1, compactAndDelete(Duration.ofDays(1)));
            case VESSEL_STATIC, TRACK_POINTS -> topic(name, PARTITIONS, delete(Duration.ofDays(7)));
            case COVERAGE_CELLS -> topic(name, PARTITIONS, compactAndDelete(Duration.ofDays(7)));
            case DETECTIONS -> topic(name, PARTITIONS, delete(Duration.ofDays(30)));
            case RULES_APPROVED -> topic(name, 1, Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));
            default -> throw new IllegalArgumentException("Unknown topic " + name);
        };
    }

    private static NewTopic topic(String name, int partitions, Map<String, String> configs) {
        return new NewTopic(name, partitions, REPLICAS).configs(configs);
    }

    private static Map<String, String> delete(Duration retention) {
        return Map.of(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retention.toMillis()));
    }

    private static Map<String, String> compactAndDelete(Duration retention) {
        return Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE,
                TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retention.toMillis()));
    }
}
