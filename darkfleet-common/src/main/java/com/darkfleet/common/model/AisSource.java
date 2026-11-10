package com.darkfleet.common.model;

public enum AisSource {
    AISSTREAM("aisstream.io", "Live AIS WebSocket (Baltic + Bosphorus)"),
    DIGITRAFFIC("Fintraffic Digitraffic", "Live AIS MQTT/REST (Gulf of Finland, CC BY 4.0: Fintraffic / digitraffic.fi)"),
    DMA_REPLAY("Danish Maritime Authority / Replay", "Historical AIS CSV replay (marked historical)");

    private final String displayName;
    private final String description;

    AisSource(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }
}
