package com.darkfleet.common.model;

/**
 * What Track &amp; Detection can flag. The wording is deliberately neutral: a detection is an
 * observation for an analyst to look at, never a finding of wrongdoing.
 */
public enum DetectionType {
    /** A vessel stopped reporting while the feeds covering its position kept working. */
    UNEXPLAINED_GAP("Unexplained AIS gap"),
    /** A cargo vessel or tanker moving slowly for a long time away from ports and anchorages. */
    LOITERING("Loitering"),
    /** Two cargo vessels or tankers close together and slow for a long time away from ports and anchorages. */
    POTENTIAL_ENCOUNTER("Potential encounter"),
    /** One MMSI broadcasting several IMO numbers, or one IMO number broadcast by several MMSIs. */
    IDENTITY_INCONSISTENCY("Identity inconsistency");

    private final String label;

    DetectionType(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
