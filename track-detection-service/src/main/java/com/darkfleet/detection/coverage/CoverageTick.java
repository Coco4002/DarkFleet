package com.darkfleet.detection.coverage;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;

import java.time.Instant;

/** One position report, sent to the partition of the H3 cell it falls in. */
public record CoverageTick(String mmsi, AisSource source, Instant time, boolean stopped) {

    /** Below this speed, or moored or at anchor, a vessel counts as stopped. */
    public static final double STOPPED_KNOTS = 0.5;

    public static CoverageTick of(NormalizedAisMessage m) {
        Integer status = m.navStatus();
        boolean stopped = m.sogKnots() != null && m.sogKnots() < STOPPED_KNOTS
                || status != null && (status == 1 || status == 5);
        return new CoverageTick(m.mmsi(), m.source(), m.eventTime(), stopped);
    }
}
