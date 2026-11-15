package com.darkfleet.detection.support;

import com.uber.h3core.H3Core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The H3 grid at resolution 6: hexagons about 3.7 km across the edge, 36 km² each. Coverage is
 * counted per cell, and encounter candidates are grouped by cell.
 */
public final class Grid {

    public static final int RESOLUTION = 6;
    private static final H3Core H3 = load();

    private Grid() {
    }

    public static String cell(double lat, double lon) {
        return H3.latLngToCellAddress(lat, lon, RESOLUTION);
    }

    /**
     * The cells a circle of {@code radiusMeters} around the point reaches: the point's own cell first,
     * then any neighbour within the radius (sampled in eight directions on the circle).
     */
    public static Set<String> cellsWithin(double lat, double lon, double radiusMeters) {
        Set<String> cells = new LinkedHashSet<>();
        cells.add(cell(lat, lon));
        for (int bearing = 0; bearing < 360; bearing += 45) {
            double[] p = Geo.destination(lat, lon, bearing, radiusMeters);
            cells.add(cell(p[0], p[1]));
        }
        return cells;
    }

    private static H3Core load() {
        try {
            return H3Core.newInstance();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not load the H3 native library", e);
        }
    }
}
