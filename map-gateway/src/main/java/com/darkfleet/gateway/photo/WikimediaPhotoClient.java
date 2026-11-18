package com.darkfleet.gateway.photo;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.gateway.GatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Looks up a freely licensed vessel photo on Wikimedia Commons under {@code Category:IMO <7 digits>},
 * returning the image URL, page URL and author/licence attribution. Results (including "no photo found")
 * are cached in memory so clicking the same ship never hammers Wikimedia. Never falls back to proprietary
 * ship-photo sites.
 */
@Component
public class WikimediaPhotoClient {

    private static final Logger log = LoggerFactory.getLogger(WikimediaPhotoClient.class);
    private static final Pattern VALID_IMO = Pattern.compile("[1-9][0-9]{6}");
    private static final Pattern HTML_TAGS = Pattern.compile("<[^>]*>");
    private static final int MAX_CACHE_ENTRIES = 5_000;

    public record VesselPhoto(
            String imageUrl,
            String descriptionUrl,
            String author,
            String license,
            String licenseUrl,
            String credit
    ) {
    }

    private record CachedPhoto(VesselPhoto photo, Instant expiresAt) {
    }

    private final GatewayProperties properties;
    private final Clock clock;
    private final HttpClient http;
    private final Map<String, CachedPhoto> cache = new ConcurrentHashMap<>();

    public WikimediaPhotoClient(GatewayProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
        this.http = HttpClient.newBuilder()
                .connectTimeout(properties.wikimediaTimeout())
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    /**
     * Returns a freely licensed Wikimedia Commons photo for {@code imoNumber}, or {@code null} when
     * the vessel has no IMO number, Wikimedia has no photo in {@code Category:IMO nnnnnnn}, or the
     * lookup fails (the UI then renders its ship-type silhouette fallback).
     */
    public VesselPhoto lookup(String imoNumber) {
        if (imoNumber == null || !VALID_IMO.matcher(imoNumber).matches()) {
            return null;
        }
        if (properties.wikimediaApiUrl().isBlank()) {
            return null;
        }
        Instant now = clock.instant();
        CachedPhoto cached = cache.get(imoNumber);
        if (cached != null && now.isBefore(cached.expiresAt())) {
            return cached.photo();
        }
        if (cache.size() >= MAX_CACHE_ENTRIES) {
            cache.entrySet().removeIf(e -> !now.isBefore(e.getValue().expiresAt()));
        }
        VesselPhoto fetched = fetch(imoNumber);
        Duration ttl = fetched != null ? properties.photoCacheTtl()
                : min(properties.photoCacheTtl(), Duration.ofHours(1));
        cache.put(imoNumber, new CachedPhoto(fetched, now.plus(ttl)));
        return fetched;
    }

    private VesselPhoto fetch(String imoNumber) {
        String categoryTitle = "Category:IMO " + imoNumber;
        JsonNode root = queryCategory(categoryTitle);
        VesselPhoto direct = parseResponse(root);
        if (direct != null) {
            return direct;
        }
        String subcategory = firstSubcategoryTitle(root);
        if (subcategory == null) {
            return null;
        }
        return parseResponse(queryCategory(subcategory));
    }

    private JsonNode queryCategory(String categoryTitle) {
        String category = URLEncoder.encode(categoryTitle, StandardCharsets.UTF_8);
        String query = "action=query&generator=categorymembers&gcmtitle=" + category
                + "&gcmtype=file%7Csubcat&gcmlimit=10&prop=imageinfo&iiprop=url%7Cextmetadata&iiurlwidth=800&format=json";
        String baseUrl = properties.wikimediaApiUrl();
        String separator = baseUrl.contains("?") ? "&" : "?";
        URI uri = URI.create(baseUrl + separator + query);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(properties.wikimediaTimeout())
                .header("User-Agent", "DarkFleet/0.1 (maritime AIS research platform)")
                .header("Accept", "application/json")
                .GET()
                .build();
        try {
            HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                log.debug("Wikimedia photo lookup for {} returned HTTP {}", categoryTitle, response.statusCode());
                return null;
            }
            return JsonSerde.mapper().readTree(response.body());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.debug("Wikimedia photo lookup for {} failed: {}", categoryTitle, e.getMessage());
            return null;
        }
    }

    static String firstSubcategoryTitle(JsonNode root) {
        if (root == null) {
            return null;
        }
        JsonNode pages = root.path("query").path("pages");
        if (!pages.isObject()) {
            return null;
        }
        for (JsonNode page : pages) {
            if (page.path("ns").asInt(-1) == 14) {
                String title = text(page, "title");
                if (title != null) {
                    return title;
                }
            }
        }
        return null;
    }

    static VesselPhoto parseResponse(JsonNode root) {
        if (root == null) {
            return null;
        }
        JsonNode pages = root.path("query").path("pages");
        if (!pages.isObject()) {
            return null;
        }
        for (JsonNode page : pages) {
            String title = page.path("title").asText("");
            if (!isRasterImage(title)) {
                continue;
            }
            JsonNode infoList = page.path("imageinfo");
            if (!infoList.isArray() || infoList.isEmpty()) {
                continue;
            }
            JsonNode info = infoList.get(0);
            String imageUrl = text(info, "thumburl");
            if (imageUrl == null) {
                imageUrl = text(info, "url");
            }
            if (imageUrl == null) {
                continue;
            }
            String descriptionUrl = text(info, "descriptionurl");
            JsonNode meta = info.path("extmetadata");
            String author = cleanMeta(meta, "Artist");
            if (author == null) {
                author = cleanMeta(meta, "Credit");
            }
            String license = cleanMeta(meta, "LicenseShortName");
            if (license == null) {
                license = cleanMeta(meta, "UsageTerms");
            }
            String licenseUrl = cleanMeta(meta, "LicenseUrl");
            String credit = buildCredit(author, license);
            return new VesselPhoto(imageUrl, descriptionUrl, author, license, licenseUrl, credit);
        }
        return null;
    }

    private static boolean isRasterImage(String title) {
        String lower = title.toLowerCase(Locale.ROOT);
        return lower.endsWith(".jpg") || lower.endsWith(".jpeg") || lower.endsWith(".png") || lower.endsWith(".webp");
    }

    private static String text(JsonNode node, String field) {
        JsonNode child = node.path(field);
        if (child.isMissingNode() || child.isNull()) {
            return null;
        }
        String value = child.asText("").trim();
        return value.isEmpty() ? null : value;
    }

    private static String cleanMeta(JsonNode meta, String field) {
        String raw = text(meta.path(field), "value");
        if (raw == null) {
            return null;
        }
        String stripped = HTML_TAGS.matcher(raw).replaceAll("")
                .replace("&amp;", "&")
                .replace("&quot;", "\"")
                .replace("&#039;", "'")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&nbsp;", " ")
                .replaceAll("\\s+", " ")
                .trim();
        return stripped.isEmpty() ? null : stripped;
    }

    private static String buildCredit(String author, String license) {
        StringBuilder sb = new StringBuilder("Wikimedia Commons");
        if (author != null) {
            sb.append(" / ").append(author);
        }
        if (license != null) {
            sb.append(" (").append(license).append(")");
        }
        return sb.toString();
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
