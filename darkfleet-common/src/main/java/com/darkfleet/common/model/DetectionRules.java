package com.darkfleet.common.model;

/**
 * The thresholds Track &amp; Detection works with. The baseline comes from Track &amp; Detection's
 * configuration (version 0); an analyst-approved rule set arrives on {@code rules.approved} under
 * {@link #KEY} and replaces it. The latest record on the topic is the one in force: Vessel
 * Intelligence publishes approved rule sets in approval order.
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

    public DetectionRules {
        if (version < 0 || gapMinutes <= 0 || coverageMinPositions <= 0 || portMinStoppedVessels <= 0
                || loiteringMaxSpeedKnots <= 0 || loiteringMinMinutes <= 0 || encounterMaxDistanceMeters <= 0
                || encounterMaxSpeedKnots <= 0 || encounterMinMinutes <= 0 || identityWindowHours <= 0
                || draughtChangeMeters <= 0) {
            throw new IllegalArgumentException("Detection rules need a version >= 0 and positive thresholds: " + version);
        }
    }
}
