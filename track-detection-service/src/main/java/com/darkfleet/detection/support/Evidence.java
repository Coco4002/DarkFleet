package com.darkfleet.detection.support;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Evidence {

    private Evidence() {
    }

    /** A map from alternating keys and values; entries whose value is unknown (null) are left out. */
    public static Map<String, Object> of(Object... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Evidence needs key/value pairs");
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            if (keysAndValues[i + 1] != null) {
                map.put((String) keysAndValues[i], keysAndValues[i + 1]);
            }
        }
        return map;
    }

    /** Rounds for display in evidence, e.g. distances to 0.1 km. */
    public static double round(double value, int decimals) {
        double factor = Math.pow(10, decimals);
        return Math.round(value * factor) / factor;
    }
}
