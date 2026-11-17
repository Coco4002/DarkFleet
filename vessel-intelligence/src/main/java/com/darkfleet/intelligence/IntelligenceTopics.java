package com.darkfleet.intelligence;

import com.darkfleet.common.kafka.DarkFleetTopics;

/**
 * The topics this instance uses: the live ones, or their replay copies in replay mode. It reads the
 * first four and writes approved rule sets to {@link #rulesApproved}.
 */
public record IntelligenceTopics(String vesselStatic, String trackPoints, String coverageCells, String detections,
                                 String rulesApproved) {

    public static IntelligenceTopics of(IntelligenceProperties properties) {
        boolean replay = properties.replay();
        return new IntelligenceTopics(
                DarkFleetTopics.output(DarkFleetTopics.VESSEL_STATIC, replay),
                DarkFleetTopics.output(DarkFleetTopics.TRACK_POINTS, replay),
                DarkFleetTopics.output(DarkFleetTopics.COVERAGE_CELLS, replay),
                DarkFleetTopics.output(DarkFleetTopics.DETECTIONS, replay),
                // replay has its own rule topic so a rule approved while replaying never reaches live detection
                DarkFleetTopics.output(DarkFleetTopics.RULES_APPROVED, replay));
    }

    public String[] all() {
        return new String[] {vesselStatic, trackPoints, coverageCells, detections, rulesApproved};
    }
}
