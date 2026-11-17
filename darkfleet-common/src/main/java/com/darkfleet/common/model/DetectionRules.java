package com.darkfleet.common.model;

import com.darkfleet.common.kafka.JsonSerde;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * The thresholds Track &amp; Detection works with. The baseline (version 0, {@link #baseline()}) is
 * shared by Track &amp; Detection, which applies it until a rule set is approved, and Vessel
 * Intelligence, which shows it and builds rule changes on it. An analyst-approved rule set arrives on
 * {@code rules.approved} under {@link #KEY} and replaces it. The latest record on the topic is the
 * one in force: Vessel Intelligence publishes approved rule sets in approval order.
 *
 * @param gapMinutes                 silence after which a vessel's gap is checked
 * @param coverageMinPositions       position reports a feed must have delivered from a cell over the
 *                                   coverage window for that cell to count as covered by the feed
 * @param portMinStoppedVessels      a cell where at least this many vessels stopped within one hour of
 *                                   the coverage window counts as a port or anchorage
 * @param loiteringMaxSpeedKnots     slower than this counts as loitering speed
 * @param loiteringMinMinutes        how long a vessel must stay that slow
 * @param encounterMaxDistanceMeters how close two vessels must be
 * @param encounterMaxSpeedKnots     how slow both must be
 * @param encounterMinMinutes        how long they must stay together
 * @param identityWindowHours        two conflicting identity reports this close in time are a conflict
 * @param draughtChangeMeters        draught change during an encounter worth pointing out in its evidence
 */
public record DetectionRules(
        int version,
        int gapMinutes,
        int coverageMinPositions,
        int portMinStoppedVessels,
        double loiteringMaxSpeedKnots,
        int loiteringMinMinutes,
        double encounterMaxDistanceMeters,
        double encounterMaxSpeedKnots,
        int encounterMinMinutes,
        int identityWindowHours,
        double draughtChangeMeters
) {

    public static final String KEY = "detection";

    private static final String BASELINE_RESOURCE = "/darkfleet/detection-rules-baseline.json";

    /** The rules in force until an analyst approves a rule set; one file read by every service. */
    public static DetectionRules baseline() {
        return Baseline.RULES;
    }

    private static final class Baseline {
        static final DetectionRules RULES = load();

        private static DetectionRules load() {
            try (InputStream in = DetectionRules.class.getResourceAsStream(BASELINE_RESOURCE)) {
                if (in == null) {
                    throw new IllegalStateException("Missing " + BASELINE_RESOURCE);
                }
                DetectionRules rules = JsonSerde.mapper().readValue(in, DetectionRules.class);
                if (rules.version() != 0) {
                    throw new IllegalStateException("The baseline rules must be version 0");
                }
                return rules;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public DetectionRules {
        if (version < 0 || gapMinutes <= 0 || coverageMinPositions <= 0 || portMinStoppedVessels <= 0
                || !positive(loiteringMaxSpeedKnots) || loiteringMinMinutes <= 0 || !positive(encounterMaxDistanceMeters)
                || !positive(encounterMaxSpeedKnots) || encounterMinMinutes <= 0 || identityWindowHours <= 0
                || !positive(draughtChangeMeters)) {
            throw new IllegalArgumentException("Detection rules need a version >= 0 and positive, finite thresholds: " + version);
        }
    }

    /** False for zero, negative values, NaN and infinity: comparisons with NaN are always false. */
    private static boolean positive(double value) {
        return value > 0 && Double.isFinite(value);
    }
}
