package com.darkfleet.gateway.intelligence;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.web.ApiException;
import com.darkfleet.security.ApiKeyAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Read-only HTTP client to Vessel Intelligence. Passes through track history, coarse all-ships
 * hourly tracks for the time slider, and hourly H3 cell coverage using the gateway's read-only API key.
 * Has no database credentials and never calls detections, cases, watchlist or proposals.
 */
@Component
public class IntelligenceClient {

    private static final Logger log = LoggerFactory.getLogger(IntelligenceClient.class);
    private static final Pattern MMSI = Pattern.compile("\\d{9}");
    public static final Duration MAX_TRACK_WINDOW = Duration.ofHours(48);

    public record TrackPointView(
            Instant time,
            double latitude,
            double longitude,
            Double sogKnots,
            Double cogDegrees,
            Double headingDegrees,
            Integer navStatus,
            String source
    ) {
    }

    public record VesselHistoryTrack(
            String mmsi,
            String name,
            Integer shipType,
            Integer lengthMeters,
            Integer beamMeters,
            List<TrackPointView> points
    ) {
    }

    public record HistoryHourView(
            Instant hourStart,
            Instant hourEnd,
            List<VesselHistoryTrack> vessels
    ) {
    }

    public record CoverageView(
            String h3Cell,
            Instant hourStart,
            JsonNode positionsBySource,
            int vessels,
            int stoppedVessels,
            JsonNode windowPositionsBySource,
            int windowMaxStoppedVessels
    ) {
    }

    private static final TypeReference<List<TrackPointView>> TRACK_LIST = new TypeReference<>() {};
    private static final TypeReference<List<CoverageView>> COVERAGE_LIST = new TypeReference<>() {};

    private final GatewayProperties properties;
    private final HttpClient http;

    public IntelligenceClient(GatewayProperties properties) {
        this.properties = properties;
        this.http = HttpClient.newBuilder()
                .connectTimeout(properties.intelligenceTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    public List<TrackPointView> vesselTrack(String mmsi, Instant from, Instant to) {
        requireMmsi(mmsi);
        if (!from.isBefore(to)) {
            throw ApiException.badRequest("from must be before to");
        }
        if (Duration.between(from, to).compareTo(MAX_TRACK_WINDOW) > 0) {
            throw ApiException.badRequest("A track window can be at most " + MAX_TRACK_WINDOW.toHours() + " hours");
        }
        String path = "/api/v1/vessels/" + mmsi + "/track?from=" + encode(from.toString())
                + "&to=" + encode(to.toString());
        byte[] body = get(path);
        return JsonSerde.mapper().readValue(body, TRACK_LIST);
    }

    public HistoryHourView historyHour(Instant hour) {
        Instant truncated = hour.truncatedTo(ChronoUnit.HOURS);
        byte[] body = get("/api/v1/vessels/tracks?hour=" + encode(truncated.toString()));
        return JsonSerde.mapper().readValue(body, HistoryHourView.class);
    }

    public List<CoverageView> coverageHour(Instant hour) {
        Instant truncated = hour.truncatedTo(ChronoUnit.HOURS);
        byte[] body = get("/api/v1/coverage?hour=" + encode(truncated.toString()));
        return JsonSerde.mapper().readValue(body, COVERAGE_LIST);
    }

    public static void requireMmsi(String mmsi) {
        if (mmsi == null || !MMSI.matcher(mmsi).matches()) {
            throw ApiException.badRequest("An MMSI has 9 digits");
        }
    }

    private byte[] get(String pathAndQuery) {
        String base = properties.intelligenceUrl();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base + pathAndQuery))
                .timeout(properties.intelligenceTimeout())
                .header("Accept", "application/json")
                .GET();
        String key = properties.intelligenceApiKey();
        if (key != null && !key.isBlank()) {
            builder.header(ApiKeyAuthenticationFilter.HEADER, key);
        }
        try {
            HttpResponse<byte[]> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            int status = response.statusCode();
            if (status == 200) {
                return response.body();
            }
            if (status == 400) {
                throw ApiException.badRequest(extractDetail(response.body(), "Invalid request"));
            }
            if (status == 404) {
                throw ApiException.notFound(extractDetail(response.body(), "Not found"));
            }
            log.warn("Vessel Intelligence returned HTTP {} for {}", status, pathAndQuery);
            throw ApiException.unavailable("History service is temporarily unavailable");
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Call to Vessel Intelligence {} failed: {}", pathAndQuery, e.getMessage());
            throw ApiException.unavailable("History service is temporarily unavailable");
        }
    }

    private static String extractDetail(byte[] body, String fallback) {
        try {
            JsonNode node = JsonSerde.mapper().readTree(body);
            JsonNode detail = node.path("detail");
            if (detail.isTextual() && !detail.asText().isBlank()) {
                return detail.asText();
            }
        } catch (Exception ignored) {
        }
        return fallback;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
