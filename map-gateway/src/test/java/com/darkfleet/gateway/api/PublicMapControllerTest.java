package com.darkfleet.gateway.api;

import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.FeedHealth;
import com.darkfleet.common.model.TrackPoint;
import com.darkfleet.common.model.VesselStatic;
import com.darkfleet.gateway.live.LiveSnapshotStore;
import com.darkfleet.security.ApiKeyAuthenticationFilter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests the public HTTP and SSE API, security rules, Vessel Intelligence history pass-through and
 * Wikimedia Commons photo attribution without needing a live Kafka container.
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
@AutoConfigureMockMvc
public class PublicMapControllerTest {

    private static final String GATEWAY_KEY = "test-gateway-ro-key";
    private static final String MMSI = "230123456";
    private static final String EXPIRED_MMSI = "273999999";
    private static final String IMO = "9308065";
    private static final String CELL = "861126d37ffffff";

    private static final HttpServer BACKENDS;
    private static final AtomicReference<String> LAST_VI_API_KEY = new AtomicReference<>();

    static {
        try {
            BACKENDS = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            BACKENDS.createContext("/vi/api/v1/vessels/" + MMSI + "/track", exchange -> {
                LAST_VI_API_KEY.set(exchange.getRequestHeaders().getFirst(ApiKeyAuthenticationFilter.HEADER));
                respondJson(exchange, 200, """
                        [
                          {"time":"2026-11-18T11:00:00Z","latitude":60.05,"longitude":24.85,"sogKnots":11.8,"cogDegrees":88.0,"headingDegrees":89.0,"navStatus":0,"source":"DIGITRAFFIC"},
                          {"time":"2026-11-18T11:30:00Z","latitude":60.10,"longitude":24.95,"sogKnots":12.1,"cogDegrees":90.0,"headingDegrees":90.0,"navStatus":0,"source":"DIGITRAFFIC"}
                        ]""");
            });
            BACKENDS.createContext("/vi/api/v1/vessels/999999999/track", exchange ->
                    respondJson(exchange, 200, "[]"));
            BACKENDS.createContext("/vi/api/v1/vessels/tracks", exchange -> {
                LAST_VI_API_KEY.set(exchange.getRequestHeaders().getFirst(ApiKeyAuthenticationFilter.HEADER));
                respondJson(exchange, 200, """
                        {
                          "hourStart": "2026-11-12T10:00:00Z",
                          "hourEnd": "2026-11-12T11:00:00Z",
                          "vessels": [
                            {
                              "mmsi": "230123456",
                              "name": "BALTIC EXPRESS",
                              "shipType": 80,
                              "lengthMeters": 244,
                              "beamMeters": 42,
                              "points": [
                                {"time":"2026-11-12T10:00:00Z","latitude":60.0,"longitude":24.8,"sogKnots":12.0,"cogDegrees":90.0,"headingDegrees":90.0,"navStatus":0,"source":"DIGITRAFFIC"},
                                {"time":"2026-11-12T10:05:00Z","latitude":60.02,"longitude":24.83,"sogKnots":12.0,"cogDegrees":90.0,"headingDegrees":90.0,"navStatus":0,"source":"DIGITRAFFIC"}
                              ]
                            }
                          ]
                        }""");
            });
            BACKENDS.createContext("/vi/api/v1/coverage", exchange -> {
                LAST_VI_API_KEY.set(exchange.getRequestHeaders().getFirst(ApiKeyAuthenticationFilter.HEADER));
                respondJson(exchange, 200, """
                        [
                          {
                            "h3Cell": "861126d37ffffff",
                            "hourStart": "2026-11-12T10:00:00Z",
                            "positionsBySource": {"DIGITRAFFIC": 320, "AISSTREAM": 14},
                            "vessels": 5,
                            "stoppedVessels": 0,
                            "windowPositionsBySource": {"DIGITRAFFIC": 6400, "AISSTREAM": 280},
                            "windowMaxStoppedVessels": 1
                          }
                        ]""");
            });
            BACKENDS.createContext("/wikimedia/w/api.php", exchange ->
                    respondJson(exchange, 200, """
                            {
                              "query": {
                                "pages": {
                                  "99": {
                                    "title": "File:Baltic Express.jpg",
                                    "imageinfo": [
                                      {
                                        "thumburl": "https://upload.wikimedia.org/commons/thumb/baltic.jpg",
                                        "descriptionurl": "https://commons.wikimedia.org/wiki/File:Baltic_Express.jpg",
                                        "extmetadata": {
                                          "Artist": {"value": "Matti Meikalainen"},
                                          "LicenseShortName": {"value": "CC BY 4.0"},
                                          "LicenseUrl": {"value": "https://creativecommons.org/licenses/by/4.0/"}
                                        }
                                      }
                                    ]
                                  }
                                }
                              }
                            }"""));
            BACKENDS.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @AfterAll
    static void stopBackends() {
        BACKENDS.stop(0);
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        int port = BACKENDS.getAddress().getPort();
        registry.add("darkfleet.gateway.intelligence-url", () -> "http://127.0.0.1:" + port + "/vi");
        registry.add("darkfleet.gateway.intelligence-api-key", () -> GATEWAY_KEY);
        registry.add("darkfleet.gateway.wikimedia-api-url", () -> "http://127.0.0.1:" + port + "/wikimedia/w/api.php");
    }

    @Autowired
    private MockMvc mvc;

    @Autowired
    private LiveSnapshotStore store;

    @Test
    void servesPublicMapEndpointsAndProtectsNonPublicRoutes() throws Exception {
        Instant now = Instant.now();
        store.onVesselStatic(new VesselStatic(MMSI, now.minusSeconds(30), AisSource.DIGITRAFFIC,
                IMO, "BALTIC EXPRESS", "OJBX", 80, 11.4, "PRIMORSK", 244, 42, now.plus(Duration.ofHours(6))));
        store.onTrackPoint(new TrackPoint(EXPIRED_MMSI, now.minus(Duration.ofHours(2)), 59.5, 24.0, 8.0, 180.0,
                180.0, 0, AisSource.AISSTREAM, CELL));
        store.onTrackPoint(new TrackPoint(MMSI, now.minusSeconds(10), 60.10, 24.95, 12.1, 90.0, 90.0, 0,
                AisSource.DIGITRAFFIC, CELL));
        store.onFeedHealth(new FeedHealth(AisSource.DIGITRAFFIC, now.minusSeconds(5), 30, true, 1050, 1000,
                now.minusSeconds(1), 50000, 0, 0));

        // Anonymous GET on snapshot
        mvc.perform(get("/api/v1/snapshot"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.vessels[*].mmsi", hasItem(MMSI)))
                .andExpect(jsonPath("$.vessels[*].mmsi", not(hasItem(EXPIRED_MMSI))))
                .andExpect(jsonPath("$.vessels[?(@.mmsi == '" + MMSI + "')].name", contains("BALTIC EXPRESS")))
                .andExpect(jsonPath("$.vessels[?(@.mmsi == '" + MMSI + "')].flagCode", contains("FI")))
                .andExpect(jsonPath("$.vessels[?(@.mmsi == '" + MMSI + "')].flagCountry", contains("Finland")))
                .andExpect(jsonPath("$.vessels[?(@.mmsi == '" + MMSI + "')].lengthMeters", contains(244)))
                .andExpect(jsonPath("$.vessels[?(@.mmsi == '" + MMSI + "')].beamMeters", contains(42)))
                .andExpect(jsonPath("$.feeds[0].source").value("DIGITRAFFIC"))
                .andExpect(jsonPath("$.feeds[0].stale").value(false));

        // Anonymous GET on SSE stream
        MvcResult sse = mvc.perform(get("/api/v1/live"))
                .andExpect(status().isOk())
                .andExpect(request().asyncStarted())
                .andReturn();
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(50))
                .until(() -> sse.getResponse().getContentAsString().contains("BALTIC EXPRESS"));

        // Anonymous GET on public ship panel
        mvc.perform(get("/api/v1/vessels/{mmsi}", MMSI))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mmsi").value(MMSI))
                .andExpect(jsonPath("$.name").value("BALTIC EXPRESS"))
                .andExpect(jsonPath("$.imo").value(IMO))
                .andExpect(jsonPath("$.callSign").value("OJBX"))
                .andExpect(jsonPath("$.flagCode").value("FI"))
                .andExpect(jsonPath("$.flagCountry").value("Finland"))
                .andExpect(jsonPath("$.lengthMeters").value(244))
                .andExpect(jsonPath("$.beamMeters").value(42))
                .andExpect(jsonPath("$.destination").value("PRIMORSK"))
                .andExpect(jsonPath("$.trail.length()").value(2))
                .andExpect(jsonPath("$.photo.imageUrl").value("https://upload.wikimedia.org/commons/thumb/baltic.jpg"))
                .andExpect(jsonPath("$.photo.credit").value("Wikimedia Commons / Matti Meikalainen (CC BY 4.0)"));
        assertEquals(GATEWAY_KEY, LAST_VI_API_KEY.get());

        // Unknown vessel returns 404; invalid MMSI returns 400
        mvc.perform(get("/api/v1/vessels/999999999")).andExpect(status().isNotFound());
        mvc.perform(get("/api/v1/vessels/not-an-mmsi")).andExpect(status().isBadRequest());

        // Anonymous GET on 1-minute vessel track
        mvc.perform(get("/api/v1/vessels/{mmsi}/track?from=2026-11-18T10:00:00Z&to=2026-11-18T12:00:00Z", MMSI))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));

        // Anonymous GET on coarse all-ships hourly history
        mvc.perform(get("/api/v1/history/tracks?hour=2026-11-12T10:25:00Z"))
                .andExpect(status().isOk())
                .andExpect(header().string(HttpHeaders.CACHE_CONTROL, containsString("max-age=60")))
                .andExpect(jsonPath("$.hourStart").value("2026-11-12T10:00:00Z"))
                .andExpect(jsonPath("$.vessels[0].mmsi").value(MMSI))
                .andExpect(jsonPath("$.vessels[0].points.length()").value(2));

        // Anonymous GET on H3 coverage hour
        mvc.perform(get("/api/v1/coverage?hour=2026-11-12T10:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(header().string(HttpHeaders.CACHE_CONTROL, containsString("max-age=60")))
                .andExpect(jsonPath("$[0].h3Cell").value(CELL))
                .andExpect(jsonPath("$[0].positionsBySource.DIGITRAFFIC").value(320));

        // OpenAPI docs are public; POST or unlisted paths require authentication
        mvc.perform(get("/v3/api-docs")).andExpect(status().isOk())
                .andExpect(jsonPath("$.info.title").value("DarkFleet Map Gateway"));
        mvc.perform(post("/api/v1/snapshot")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/v1/detections")).andExpect(status().isUnauthorized());
    }

    private static void respondJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }
}
