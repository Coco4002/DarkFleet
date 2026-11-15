package com.darkfleet.detection;

import com.darkfleet.common.kafka.DarkFleetTopics;

/**
 * Live detection reads {@code ais.raw} and keeps time by the wall clock, so a silent vessel is
 * noticed even when nothing else arrives. Replay reads {@code ais.replay}, keeps time by the
 * replayed messages, writes the {@code replay.*} copies of the output topics and has no feed
 * heartbeats to consult.
 */
public enum DetectionMode {
    LIVE(DarkFleetTopics.AIS_RAW, false),
    REPLAY(DarkFleetTopics.AIS_REPLAY, true);

    private final String inputTopic;
    private final boolean replay;

    DetectionMode(String inputTopic, boolean replay) {
        this.inputTopic = inputTopic;
        this.replay = replay;
    }

    public String inputTopic() {
        return inputTopic;
    }

    public String output(String topic) {
        return DarkFleetTopics.output(topic, replay);
    }

    /** Replayed data has no heartbeats; its feeds are taken as working throughout. */
    public boolean usesFeedHealth() {
        return !replay;
    }
}
