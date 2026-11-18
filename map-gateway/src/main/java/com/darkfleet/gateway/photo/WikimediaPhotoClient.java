package com.darkfleet.gateway.photo;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.gateway.GatewayProperties;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Looks up a freely licensed vessel photo on Wikimedia Commons under {@code Category:IMO <7 digits>},
 * returning the image URL, page URL and author/licence attribution. Results, including "no photo", are
 * cached per IMO number for {@code photoCacheTtl}; simultaneous requests for the same ship make one lookup.
 * A failed lookup is not cached, so the next click tries again. Requests go out one at a time, as Wikimedia
 * asks of API clients, and none go out while a {@code Retry-After} it sent is in force. Never uses proprietary
 * ship-photo sites.
 */
@Component
public class WikimediaPhotoClient {

    private static final Logger log = LoggerFactory.getLogger(WikimediaPhotoClient.class);
    private static final Pattern VALID_IMO = Pattern.compile("[1-9][0-9]{6}");
    private static final Pattern HTML_TAGS = Pattern.compile("<[^>]*>");
    /** More than the ships with an IMO number seen in a week across the region. */
    private static final int MAX_CACHE_ENTRIES = 10_000;

    public record VesselPhoto(
            String imageUrl,
            String descriptionUrl,
            String author,
            String license,
            String licenseUrl,
            String credit
    ) {
    }

    private final GatewayProperties properties;
    private final HttpClient http;
    private final Cache<String, Optional<VesselPhoto>> cache;
    private final Semaphore oneAtATime = new Semaphore(1);
    private final AtomicReference<Instant> notBefore = new AtomicReference<>(Instant.EPOCH);
    private final Clock clock;

    public WikimediaPhotoClient(GatewayProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
        this.http = HttpClient.newBuilder()
                .connectTimeout(properties.wikimediaTimeout())
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(properties.photoCacheTtl())
                .maximumSize(MAX_CACHE_ENTRIES)
                .build();
    }

    /**
     * Returns a freely licensed Wikimedia Commons photo for {@code imoNumber}, or {@code null} when
     * the vessel has no valid IMO number, Wikimedia has no photo in {@code Category:IMO nnnnnnn}, or the
     * lookup fails (the UI then draws the ship-type silhouette).
     */
    public VesselPhoto lookup(String imoNumber) {
        if (imoNumber == null || !VALID_IMO.matcher(imoNumber).matches()) {
            return null;
        }
        Optional<VesselPhoto> photo = cache.get(imoNumber, this::fetch);
        return photo == null ? null : photo.orElse(null);
    }

    /** The photo, empty when Commons has none, or {@code null} when a request failed (not cached). */
    private Optional<VesselPhoto> fetch(String imoNumber) {
        JsonNode root = queryCategory("Category:IMO " + imoNumber);
        if (root == null) {
            return null;
        }
        VesselPhoto direct = parseResponse(root);
        if (direct != null) {
            return Optional.of(direct);
        }
        String subcategory = firstSubcategoryTitle(root);
        if (subcategory == null) {
            return Optional.empty();
        }
        JsonNode sub = queryCategory(subcategory);
        return sub == null ? null : Optional.ofNullable(parseResponse(sub));
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
            if (!oneAtATime.tryAcquire(properties.wikimediaTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.debug("Wikimedia photo lookup for {} skipped: other lookups still running", categoryTitle);
                return null;
            }
            HttpResponse<byte[]> response;
            // checked and set while holding the permit, so a lookup waiting for it sees what this one was told
            try {
                if (clock.instant().isBefore(notBefore.get())) {
                    log.debug("Wikimedia photo lookup for {} skipped: Wikimedia asked to wait until {}",
                            categoryTitle, notBefore.get());
                    return null;
                }
                response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
                response.headers().firstValue("Retry-After").ifPresent(this::waitAsAsked);
            } finally {
                oneAtATime.release();
            }
            if (response.statusCode() != 200) {
                log.debug("Wikimedia photo lookup for {} returned HTTP {}", categoryTitle, response.statusCode());
                return null;
            }
            JsonNode root = JsonSerde.mapper().readTree(response.body());
            // the API reports its errors (throttling included) with HTTP 200 and an "error" object
            if (root.has("error")) {
                log.debug("Wikimedia photo lookup for {} failed: {}", categoryTitle, root.path("error").path("code"));
                return null;
            }
            return root;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.debug("Wikimedia photo lookup for {} failed: {}", categoryTitle, e.getMessage());
            return null;
        }
    }

    /** {@code Retry-After} is either seconds or an HTTP date. */
    private void waitAsAsked(String retryAfter) {
        Instant until;
        try {
            until = clock.instant().plusSeconds(Long.parseLong(retryAfter.trim()));
        } catch (NumberFormatException e) {
            try {
                until = ZonedDateTime.parse(retryAfter.trim(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            } catch (DateTimeParseException unreadable) {
                log.debug("Ignoring unreadable Retry-After from Wikimedia: {}", retryAfter);
                return;
            }
        }
        Instant asked = until;
        notBefore.accumulateAndGet(asked, (a, b) -> a.isAfter(b) ? a : b);
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

}
