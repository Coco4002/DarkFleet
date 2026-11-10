package com.darkfleet.common.model;

public enum CorridorRegion {
    BALTIC_CORRIDOR("Gulf of Finland to Danish Straits", 53.5, 9.0, 60.8, 30.5),
    BOSPHORUS_BLACK_SEA("Bosphorus & Western Black Sea", 40.8, 27.5, 44.5, 32.5),
    OUTSIDE_CORRIDORS("Outside Monitored Corridors", -90.0, -180.0, 90.0, 180.0);

    private final String label;
    private final double minLat;
    private final double minLon;
    private final double maxLat;
    private final double maxLon;

    CorridorRegion(String label, double minLat, double minLon, double maxLat, double maxLon) {
        this.label = label;
        this.minLat = minLat;
        this.minLon = minLon;
        this.maxLat = maxLat;
        this.maxLon = maxLon;
    }

    public String getLabel() {
        return label;
    }

    public double getMinLat() {
        return minLat;
    }

    public double getMinLon() {
        return minLon;
    }

    public double getMaxLat() {
        return maxLat;
    }

    public double getMaxLon() {
        return maxLon;
    }

    public boolean contains(double lat, double lon) {
        return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
    }

    public static CorridorRegion classify(double lat, double lon) {
        if (BALTIC_CORRIDOR.contains(lat, lon)) {
            return BALTIC_CORRIDOR;
        }
        if (BOSPHORUS_BLACK_SEA.contains(lat, lon)) {
            return BOSPHORUS_BLACK_SEA;
        }
        return OUTSIDE_CORRIDORS;
    }
}
