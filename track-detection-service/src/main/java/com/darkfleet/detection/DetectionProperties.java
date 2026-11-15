package com.darkfleet.detection;

import com.darkfleet.common.model.DetectionRules;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

/**
 * {@code darkfleet.detection.*}. Thresholds an analyst may change are in {@link DetectionRules};
 * these are the service's own operating settings.
 *
 * @param checkInterval         how often silent vessels, finished episodes and completed hours are checked
 * @param feedStaleAfter        a feed whose last heartbeat or last message is older than this is not working
 * @param maxClockSkew          messages stamped further than this ahead of their reception are dropped
 * @param trackPointInterval    a track point is published at least this often per vessel ...
 * @param trackTurnDegrees      ... or when its course changed this much since the last one ...
 * @param trackSpeedChangeKnots ... or its speed this much
 * @param staticRefresh         unchanged static data is published again after this long
 * @param forgetAfter           a vessel not heard from for this long is dropped, closing what is still open
 * @param episodeBreak          loitering or an encounter without a qualifying report for this long is over
 * @param coverageGrace         how long after the end of an hour late reports are still counted for it
 * @param baselineRules         rules in force until an approved rule set arrives
 */
@ConfigurationProperties(prefix = "darkfleet.detection")
public record DetectionProperties(
        DetectionMode mode,
        Duration checkInterval,
        Duration feedStaleAfter,
        Duration maxClockSkew,
        Duration trackPointInterval,
        double trackTurnDegrees,
        double trackSpeedChangeKnots,
        Duration staticRefresh,
        Duration forgetAfter,
        Duration episodeBreak,
        Duration coverageGrace,
        DetectionRules baselineRules
) {

    public DetectionProperties {
        Objects.requireNonNull(mode, "darkfleet.detection.mode is required");
        Objects.requireNonNull(checkInterval, "darkfleet.detection.check-interval is required");
        Objects.requireNonNull(feedStaleAfter, "darkfleet.detection.feed-stale-after is required");
        Objects.requireNonNull(maxClockSkew, "darkfleet.detection.max-clock-skew is required");
        Objects.requireNonNull(trackPointInterval, "darkfleet.detection.track-point-interval is required");
        Objects.requireNonNull(staticRefresh, "darkfleet.detection.static-refresh is required");
        Objects.requireNonNull(forgetAfter, "darkfleet.detection.forget-after is required");
        Objects.requireNonNull(episodeBreak, "darkfleet.detection.episode-break is required");
        Objects.requireNonNull(coverageGrace, "darkfleet.detection.coverage-grace is required");
        Objects.requireNonNull(baselineRules, "darkfleet.detection.baseline-rules is required");
        if (trackTurnDegrees <= 0 || trackSpeedChangeKnots <= 0) {
            throw new IllegalArgumentException("darkfleet.detection.track-turn-degrees and track-speed-change-knots must be positive");
        }
    }
}
