package com.darkfleet.common.model;

/**
 * Candidate event types produced by Track & Detection.
 * Neutral terminology is strictly enforced: findings are candidate observations, never proven wrongdoing.
 */
public enum DetectionType {
    UNEXPLAINED_GAP("Unexplained AIS Gap", "AIS transmission gap in a well-covered cell not seen on alternate feeds"),
    COVERAGE_GAP_SUPPRESSED("Cross-Source Coverage Gap (Suppressed)", "Vessel lost on one feed but confirmed active by secondary feed or located in low-coverage cell"),
    LOITERING("Offshore Loitering Candidate", "Prolonged low-speed manoeuvring by cargo/tanker vessel outside port and anchorage zones"),
    POTENTIAL_ENCOUNTER("Potential Ship-to-Ship Encounter", "Prolonged close-proximity encounter between two cargo/tanker vessels (AIS 70-89) offshore"),
    DRAUGHT_CHANGE("Post-Encounter Draught Change", "Reported static draught change following a potential ship-to-ship encounter"),
    IMPOSSIBLE_JUMP("Impossible Position Jump (Low Confidence)", "Apparent vessel speed between consecutive reports exceeds physical capability"),
    POSITION_ON_LAND("Position on Land Signal (Low Confidence)", "Reported coordinates fall inside land mask polygon"),
    IDENTITY_INCONSISTENCY("Identity Inconsistency Signal (Low Confidence)", "Conflicting IMO, vessel name, or ship type broadcast under the same MMSI or shared IMO across MMSIs");

    private final String neutralLabel;
    private final String description;

    DetectionType(String neutralLabel, String description) {
        this.neutralLabel = neutralLabel;
        this.description = description;
    }

    public String getNeutralLabel() {
        return neutralLabel;
    }

    public String getDescription() {
        return description;
    }
}
