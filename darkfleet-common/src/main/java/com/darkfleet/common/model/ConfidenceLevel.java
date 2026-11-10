package com.darkfleet.common.model;

public enum ConfidenceLevel {
    LOW(0.30, "Lower-confidence signal requiring cross-check"),
    MEDIUM(0.65, "Moderate-confidence candidate anomaly"),
    HIGH(0.85, "High-priority multi-factor candidate event");

    private final double defaultScore;
    private final String description;

    ConfidenceLevel(double defaultScore, String description) {
        this.defaultScore = defaultScore;
        this.description = description;
    }

    public double getDefaultScore() {
        return defaultScore;
    }

    public String getDescription() {
        return description;
    }
}
