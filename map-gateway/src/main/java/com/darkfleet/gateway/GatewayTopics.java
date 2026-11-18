package com.darkfleet.gateway;

import com.darkfleet.common.kafka.DarkFleetTopics;

/**
 * The topics this instance reads: the live ones, or their replay copies in replay mode.
 * {@code feed.health} has no replay copy because replayed files have no live adapter heartbeats.
 */
public record GatewayTopics(String vesselStatic, String trackPoints, String feedHealth) {

    public static GatewayTopics of(GatewayProperties properties) {
        boolean replay = properties.replay();
        return new GatewayTopics(
                DarkFleetTopics.output(DarkFleetTopics.VESSEL_STATIC, replay),
                DarkFleetTopics.output(DarkFleetTopics.TRACK_POINTS, replay),
                DarkFleetTopics.FEED_HEALTH);
    }

    public String[] all() {
        return new String[] {vesselStatic, trackPoints, feedHealth};
    }
}
