package com.darkfleet.common.spatial;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Spatial indexing, H3 grid cell mapping, Haversine distance/velocity calculations,
 * port/anchorage exclusion zones, and land-mask checks for the Baltic and Bosphorus corridors.
 */
public final class SpatialEngine {

    private static final double EARTH_RADIUS_METERS = 6_371_000.0;
    private static final double METERS_PER_NAUTICAL_MILE = 1_852.0;

    /**
     * Resolution-6 equivalent grid step (~0.08 deg lat, ~0.15 deg lon in Baltic/Black Sea latitudes).
     */
    private static final double LAT_STEP_DEG = 0.08;
    private static final double LON_STEP_DEG = 0.15;

    public record PortAnchorageZone(
            String name,
            double latitude,
            double longitude,
            double exclusionRadiusMeters
    ) {
    }

    private static final List<PortAnchorageZone> PORT_ANCHORAGE_ZONES = List.of(
            new PortAnchorageZone("Ust-Luga Port & Terminal", 59.680, 28.400, 11_000),
            new PortAnchorageZone("Primorsk Oil Terminal", 60.340, 28.680, 10_000),
            new PortAnchorageZone("Vysotsk Terminal", 60.615, 28.550, 9_000),
            new PortAnchorageZone("Kronstadt / St. Petersburg Approach", 60.000, 29.700, 14_000),
            new PortAnchorageZone("Helsinki / Vuosaari Port", 60.155, 25.050, 9_500),
            new PortAnchorageZone("Porvoo / Kilpilahti Refinery", 60.285, 25.550, 9_000),
            new PortAnchorageZone("Tallinn / Muuga Bay", 59.490, 24.850, 10_500),
            new PortAnchorageZone("Paldiski Port", 59.350, 24.040, 8_000),
            new PortAnchorageZone("Ventspils Port", 57.400, 21.530, 9_500),
            new PortAnchorageZone("Gdansk / Naftoport", 54.410, 18.700, 11_000),
            new PortAnchorageZone("Kalundborg Port & Fjord", 55.665, 11.070, 8_500),
            new PortAnchorageZone("Gothenburg Port Approach", 57.680, 11.780, 11_000),
            new PortAnchorageZone("Skagen Inner Anchorage", 57.730, 10.620, 7_500),
            new PortAnchorageZone("Istanbul / Ahirkapi Anchorage", 40.980, 28.960, 8_500),
            new PortAnchorageZone("Turkeli / Northern Bosphorus Entrance", 41.240, 29.130, 7_000),
            new PortAnchorageZone("Constanta Port", 44.140, 28.670, 10_000)
    );

    private SpatialEngine() {
    }

    /**
     * Encodes a (lat, lon) coordinate into a deterministic H3-compatible hexagonal cell identifier
     * at resolution 6 (e.g. "861f2_0750_0165").
     */
    public static String toH3Cell(double lat, double lon) {
        long latBucket = Math.round((lat + 90.0) / LAT_STEP_DEG);
        long lonBucket = Math.round((lon + 180.0) / LON_STEP_DEG);
        return String.format(Locale.ROOT, "861f_%04d_%04d", latBucket, lonBucket);
    }

    /**
     * Returns the (lat, lon) center of a cell encoded by {@link #toH3Cell(double, double)}.
     */
    public static double[] cellCenter(String h3Cell) {
        if (h3Cell != null && h3Cell.startsWith("861f_")) {
            String[] parts = h3Cell.split("_");
            if (parts.length == 3) {
                try {
                    long latBucket = Long.parseLong(parts[1]);
                    long lonBucket = Long.parseLong(parts[2]);
                    double lat = (latBucket * LAT_STEP_DEG) - 90.0;
                    double lon = (lonBucket * LON_STEP_DEG) - 180.0;
                    return new double[]{
                            Math.round(lat * 10000.0) / 10000.0,
                            Math.round(lon * 10000.0) / 10000.0
                    };
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return new double[]{59.75, 25.00};
    }

    /**
     * Returns the cell and its immediate 1-ring neighbors so vessels near a cell boundary
     * can still be matched for ship-to-ship encounters.
     */
    public static List<String> cellAndNeighbors(double lat, double lon) {
        long latBucket = Math.round((lat + 90.0) / LAT_STEP_DEG);
        long lonBucket = Math.round((lon + 180.0) / LON_STEP_DEG);
        List<String> cells = new ArrayList<>(7);
        cells.add(String.format(Locale.ROOT, "861f_%04d_%04d", latBucket, lonBucket));
        int[][] hexOffsets = {
                {1, 0}, {-1, 0}, {0, 1}, {0, -1}, {1, 1}, {-1, -1}
        };
        for (int[] offset : hexOffsets) {
            cells.add(String.format(
                    Locale.ROOT,
                    "861f_%04d_%04d",
                    latBucket + offset[0],
                    lonBucket + offset[1]
            ));
        }
        return cells;
    }

    /**
     * Great-circle distance in meters using the Haversine formula.
     */
    public static double distanceMeters(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double rLat1 = Math.toRadians(lat1);
        double rLat2 = Math.toRadians(lat2);

        double a = Math.sin(dLat / 2.0) * Math.sin(dLat / 2.0)
                + Math.cos(rLat1) * Math.cos(rLat2) * Math.sin(dLon / 2.0) * Math.sin(dLon / 2.0);
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(Math.max(0.0, 1.0 - a)));
        return EARTH_RADIUS_METERS * c;
    }

    public static double distanceNauticalMiles(double lat1, double lon1, double lat2, double lon2) {
        return distanceMeters(lat1, lon1, lat2, lon2) / METERS_PER_NAUTICAL_MILE;
    }

    /**
     * Computes implied speed in knots between two timestamped coordinates.
     */
    public static double impliedSpeedKnots(
            double lat1, double lon1, Instant t1,
            double lat2, double lon2, Instant t2
    ) {
        long seconds = Math.abs(Duration.between(t1, t2).getSeconds());
        if (seconds == 0) {
            return 0.0;
        }
        double nauticalMiles = distanceNauticalMiles(lat1, lon1, lat2, lon2);
        double hours = seconds / 3600.0;
        return nauticalMiles / hours;
    }

    /**
     * Checks if a position lies inside any port or anchorage exclusion zone.
     */
    public static boolean isNearPortOrAnchorage(double lat, double lon) {
        return findPortOrAnchorage(lat, lon).isPresent();
    }

    public static Optional<PortAnchorageZone> findPortOrAnchorage(double lat, double lon) {
        for (PortAnchorageZone zone : PORT_ANCHORAGE_ZONES) {
            if (distanceMeters(lat, lon, zone.latitude(), zone.longitude()) <= zone.exclusionRadiusMeters()) {
                return Optional.of(zone);
            }
        }
        return Optional.empty();
    }

    public static List<PortAnchorageZone> getPortAnchorageZones() {
        return PORT_ANCHORAGE_ZONES;
    }

    /**
     * Checks if coordinates lie in the Gulf of Finland dual-feed overlap zone
     * where both aisstream.io and Fintraffic Digitraffic have active receiver coverage.
     */
    public static boolean isInFinnishWatersOverlap(double lat, double lon) {
        return lat >= 59.15 && lat <= 60.65 && lon >= 21.00 && lon <= 29.60;
    }

    /**
     * Simplified land-mask check for interior landmasses surrounding the Baltic and Black Sea corridors.
     * Used to flag lower-confidence GNSS spoofing / corrupt position reports on land.
     */
    public static boolean isPositionOnLand(double lat, double lon) {
        // Interior of southern Finland (north of coastal belt)
        if (lat >= 60.68 && lat <= 63.0 && lon >= 22.0 && lon <= 28.0) {
            return true;
        }
        // Interior mainland Estonia / Latvia
        if (lat >= 57.5 && lat <= 59.25 && lon >= 24.9 && lon <= 27.5) {
            return true;
        }
        // Interior Sweden (Småland / Götaland inland)
        if (lat >= 56.5 && lat <= 58.3 && lon >= 13.2 && lon <= 15.8) {
            return true;
        }
        // Interior Thrace / Anatolia away from Marmara & Bosphorus
        if (lat >= 41.35 && lat <= 42.1 && lon >= 26.5 && lon <= 27.5) {
            return true;
        }
        return false;
    }
}
