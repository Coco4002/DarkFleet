package com.darkfleet.adapter.aisstream;

import com.darkfleet.adapter.support.FeedPublisher;
import com.darkfleet.common.kafka.JsonSerde;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class AisStreamFeedServiceTest {

    @Test
    void subscriptionUsesTheConfiguredAreasInAisstreamFormat() {
        var properties = new AisStreamProperties("wss://stream.aisstream.io/v0/stream", "test-key", true,
                List.of(List.of(53.5, 9.0, 60.8, 30.5), List.of(40.8, 27.5, 44.5, 32.5)));
        var service = new AisStreamFeedService(properties, new AisStreamNormalizer(), mock(FeedPublisher.class));

        JsonNode payload = JsonSerde.mapper().readTree(service.buildSubscriptionPayload());

        assertEquals("test-key", payload.path("APIKey").asString());
        // each box is [[lat1, lon1], [lat2, lon2]]
        assertEquals(53.5, payload.at("/BoundingBoxes/0/0/0").asDouble());
        assertEquals(9.0, payload.at("/BoundingBoxes/0/0/1").asDouble());
        assertEquals(60.8, payload.at("/BoundingBoxes/0/1/0").asDouble());
        assertEquals(30.5, payload.at("/BoundingBoxes/0/1/1").asDouble());
        assertEquals(2, payload.path("BoundingBoxes").size());
        assertEquals(3, payload.path("FilterMessageTypes").size());
    }

    @Test
    void rejectsMalformedAreas() {
        assertThrows(IllegalArgumentException.class,
                () -> new AisStreamProperties("wss://x", "k", true, List.of(List.of(60.8, 9.0, 53.5, 30.5))));
        assertThrows(IllegalArgumentException.class,
                () -> new AisStreamProperties("wss://x", "k", true, List.of(List.of(53.5, 9.0, 60.8))));
        assertThrows(IllegalArgumentException.class, () -> new AisStreamProperties("wss://x", "k", true, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new AisStreamProperties("wss://x", "k", true, null));
    }
}
