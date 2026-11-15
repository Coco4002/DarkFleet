package com.darkfleet.common.model;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Reception statistics of one H3 cell (resolution 6) on {@code coverage.cells}, published for every
 * completed hour while the cell had traffic within the last {@link #WINDOW} (an hour without traffic
 * has empty hour counts). Besides the hour itself it carries the totals of the window ending with
 * that hour, which is what "this cell is normally covered" means.
 *
 * @param positionsBySource    position reports received in the hour, per feed
 * @param vessels              distinct vessels reporting from the cell in the hour
 * @param stoppedVessels       distinct vessels there that were stopped (moored, anchored or below 0.5 kn)
 * @param windowPositionsBySource position reports per feed over the window
 * @param windowMaxStoppedVessels the highest hourly {@code stoppedVessels} in the window; a cell where
 *                             many vessels stop is a port or an anchorage
 */
public record CoverageCell(
        String h3Cell,
        Instant hourStart,
        Map<AisSource, Long> positionsBySource,
        int vessels,
        int stoppedVessels,
        Map<AisSource, Long> windowPositionsBySource,
        int windowMaxStoppedVessels
) {

    public static final Duration WINDOW = Duration.ofHours(24);

    public CoverageCell {
        Objects.requireNonNull(h3Cell, "h3Cell");
        Objects.requireNonNull(hourStart, "hourStart");
        positionsBySource = Map.copyOf(positionsBySource);
        windowPositionsBySource = Map.copyOf(windowPositionsBySource);
    }

    public long windowPositions(AisSource source) {
        return windowPositionsBySource.getOrDefault(source, 0L);
    }
}
