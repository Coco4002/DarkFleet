package com.darkfleet.common.model;

public enum AisMessageType {
    POSITION_REPORT(1, "Position Report (Types 1, 2, 3, 18)"),
    STATIC_VOYAGE(5, "Static and Voyage Related Data (Type 5, 24)");

    private final int defaultAisId;
    private final String description;

    AisMessageType(int defaultAisId, String description) {
        this.defaultAisId = defaultAisId;
        this.description = description;
    }

    public int getDefaultAisId() {
        return defaultAisId;
    }

    public String getDescription() {
        return description;
    }
}
