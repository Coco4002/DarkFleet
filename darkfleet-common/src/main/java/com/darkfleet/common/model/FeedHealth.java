package com.darkfleet.common.model;

import java.time.Instant;

/**
 * Heartbeat a live feed adapter publishes to {@code feed.health} at a fixed interval, keyed by source.
 * Lets consumers tell "the whole feed is down" apart from "one ship went silent".
 *
 * @param intervalSeconds      how often heartbeats are sent; a missing heartbeat for a few intervals
 *                             means the adapter itself is down
 * @param messagesInInterval   messages published since the previous heartbeat
 * @param positionsInInterval  position reports among them
 * @param lastMessageAt        when the feed last delivered anything, null if never
 */
public record FeedHealth(
        AisSource source,
        Instant emittedAt,
        int intervalSeconds,
        boolean connected,
        long messagesInInterval,
        long positionsInInterval,
        Instant lastMessageAt,
        long totalPublished,
        long totalSendFailures,
        long totalReconnects
) {
}
