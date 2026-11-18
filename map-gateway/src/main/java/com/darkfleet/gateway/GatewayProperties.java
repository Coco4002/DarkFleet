package com.darkfleet.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

/**
 * @param replay              true when this instance serves a stack in replay mode: it reads the {@code replay.*}
 *                            topics Track &amp; Detection writes from replayed data, never the live ones
 * @param vesselExpiry        how long a vessel stays in the live snapshot after its last position report;
 *                            on startup {@code track.points} is rewound by the same duration
 * @param snapshotInterval    how often the full live snapshot is pushed to connected browsers over SSE
 * @param feedStaleAfter      a live feed whose heartbeat or last message is older than this is marked stale
 * @param intelligenceUrl     base URL of Vessel Intelligence (read-only history and coverage pass-through)
 * @param intelligenceApiKey  read-only API key sent to Vessel Intelligence in {@code X-Api-Key}
 * @param intelligenceTimeout timeout for HTTP calls to Vessel Intelligence
 * @param wikimediaApiUrl     Wikimedia Commons Action API endpoint for vessel photo lookups by IMO category
 * @param wikimediaTimeout    timeout for Wikimedia Commons photo lookups
 * @param photoCacheTtl       how long photo lookup results (including "no photo") are kept in memory
 */
@ConfigurationProperties("darkfleet.gateway")
public record GatewayProperties(
        boolean replay,
        Duration vesselExpiry,
        Duration snapshotInterval,
        Duration feedStaleAfter,
        String intelligenceUrl,
        String intelligenceApiKey,
        Duration intelligenceTimeout,
        String wikimediaApiUrl,
        Duration wikimediaTimeout,
        Duration photoCacheTtl
) {

    public GatewayProperties {
        Objects.requireNonNull(vesselExpiry, "darkfleet.gateway.vessel-expiry is required");
        Objects.requireNonNull(snapshotInterval, "darkfleet.gateway.snapshot-interval is required");
        Objects.requireNonNull(feedStaleAfter, "darkfleet.gateway.feed-stale-after is required");
        Objects.requireNonNull(intelligenceUrl, "darkfleet.gateway.intelligence-url is required");
        Objects.requireNonNull(intelligenceTimeout, "darkfleet.gateway.intelligence-timeout is required");
        Objects.requireNonNull(wikimediaApiUrl, "darkfleet.gateway.wikimedia-api-url is required");
        Objects.requireNonNull(wikimediaTimeout, "darkfleet.gateway.wikimedia-timeout is required");
        Objects.requireNonNull(photoCacheTtl, "darkfleet.gateway.photo-cache-ttl is required");
        if (vesselExpiry.isNegative() || vesselExpiry.isZero()) {
            throw new IllegalArgumentException("darkfleet.gateway.vessel-expiry must be positive");
        }
        if (snapshotInterval.isNegative() || snapshotInterval.isZero()) {
            throw new IllegalArgumentException("darkfleet.gateway.snapshot-interval must be positive");
        }
        if (feedStaleAfter.isNegative() || feedStaleAfter.isZero()) {
            throw new IllegalArgumentException("darkfleet.gateway.feed-stale-after must be positive");
        }
    }
}
