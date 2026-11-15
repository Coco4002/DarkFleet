package com.darkfleet.detection.support;

/** Great-circle helpers on a spherical Earth; plenty accurate at the distances detection works with. */
public final class Geo {

    public static final double METERS_PER_NAUTICAL_MILE = 1852.0;
    private static final double EARTH_RADIUS_METERS = 6_371_008.8;

    private Geo() {
    }

    public static double distanceMeters(double lat1, double lon1, double lat2, double lon2) {
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dPhi = phi2 - phi1;
        double dLambda = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
                + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
        return 2 * EARTH_RADIUS_METERS * Math.asin(Math.min(1, Math.sqrt(a)));
    }

    /** The point reached from (lat, lon) after {@code meters} along the initial bearing; returns {lat, lon}. */
    public static double[] destination(double lat, double lon, double bearingDegrees, double meters) {
        double delta = meters / EARTH_RADIUS_METERS;
        double theta = Math.toRadians(bearingDegrees);
        double phi1 = Math.toRadians(lat);
        double lambda1 = Math.toRadians(lon);
        double phi2 = Math.asin(Math.sin(phi1) * Math.cos(delta) + Math.cos(phi1) * Math.sin(delta) * Math.cos(theta));
        double lambda2 = lambda1 + Math.atan2(Math.sin(theta) * Math.sin(delta) * Math.cos(phi1),
                Math.cos(delta) - Math.sin(phi1) * Math.sin(phi2));
        double lon2 = (Math.toDegrees(lambda2) + 540) % 360 - 180;
        return new double[]{Math.toDegrees(phi2), lon2};
    }

    /** Smallest angle between two courses, 0..180. */
    public static double courseDifference(double a, double b) {
        double d = Math.abs(a - b) % 360;
        return d > 180 ? 360 - d : d;
    }
}
