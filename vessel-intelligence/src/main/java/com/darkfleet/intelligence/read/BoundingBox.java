package com.darkfleet.intelligence.read;

import com.darkfleet.intelligence.web.ApiException;

/** An area given as {@code minLon,minLat,maxLon,maxLat} in degrees (WGS 84). */
public record BoundingBox(double minLon, double minLat, double maxLon, double maxLat) {

    /** @return null for a missing parameter; throws 400 for a malformed one */
    public static BoundingBox parse(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        String[] parts = text.split(",");
        if (parts.length != 4) {
            throw ApiException.badRequest("bbox is minLon,minLat,maxLon,maxLat");
        }
        double[] v = new double[4];
        try {
            for (int i = 0; i < 4; i++) {
                v[i] = Double.parseDouble(parts[i].trim());
            }
        } catch (NumberFormatException e) {
            throw ApiException.badRequest("bbox values must be numbers");
        }
        if (v[0] >= v[2] || v[1] >= v[3] || v[0] < -180 || v[2] > 180 || v[1] < -90 || v[3] > 90) {
            throw ApiException.badRequest("bbox must have minLon < maxLon and minLat < maxLat, within -180..180 and -90..90");
        }
        return new BoundingBox(v[0], v[1], v[2], v[3]);
    }
}
