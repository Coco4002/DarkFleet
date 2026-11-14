package com.darkfleet.adapter.aisstream;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * {@code darkfleet.aisstream.*}
 *
 * @param boundingBoxes areas to subscribe to, each as [minLat, minLon, maxLat, maxLon]
 */
@ConfigurationProperties(prefix = "darkfleet.aisstream")
public record AisStreamProperties(
        String wsUrl,
        String apiKey,
        boolean autoConnect,
        List<List<Double>> boundingBoxes
) {

    public AisStreamProperties {
        if (wsUrl == null || wsUrl.isBlank()) {
            throw new IllegalArgumentException("darkfleet.aisstream.ws-url is required");
        }
        if (boundingBoxes == null || boundingBoxes.isEmpty()) {
            throw new IllegalArgumentException("darkfleet.aisstream.bounding-boxes needs at least one area");
        }
        boundingBoxes = List.copyOf(boundingBoxes);
        for (List<Double> box : boundingBoxes) {
            if (box.size() != 4 || box.get(0) >= box.get(2) || box.get(1) >= box.get(3)) {
                throw new IllegalArgumentException(
                        "Each darkfleet.aisstream.bounding-boxes entry must be [minLat, minLon, maxLat, maxLon]: " + box);
            }
        }
    }

    public boolean hasApiKey() {
        return apiKey != null && !apiKey.isBlank();
    }
}
