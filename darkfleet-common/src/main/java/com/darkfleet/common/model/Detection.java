package com.darkfleet.common.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One state of a detection on {@code detections}, keyed by {@link #mmsi}. A detection is published
 * when it opens, and again when it closes; each publication has a higher {@link #version}, so a
 * consumer keeps the highest version per {@link #detectionId} and ignores older or repeated ones.
 *
 * @param detectionId stable id, the same for every version of this detection
 * @param otherMmsi   the second vessel of an encounter or identity conflict, otherwise null
 * @param endedAt     null while open
 * @param latitude    where it happened, null when it has no place (some identity conflicts)
 * @param rulesVersion version of the rule set that raised it
 * @param evidence    facts behind the detection (numbers and text), for the analyst and the agent
 */
public record Detection(
        String detectionId,
        DetectionType type,
        DetectionStatus status,
        int version,
        String mmsi,
        String otherMmsi,
        Instant startedAt,
        Instant endedAt,
        Double latitude,
        Double longitude,
        String h3Cell,
        int rulesVersion,
        Map<String, Object> evidence
) {

    public Detection {
        Objects.requireNonNull(detectionId, "detectionId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(mmsi, "mmsi");
        Objects.requireNonNull(startedAt, "startedAt");
        evidence = Map.copyOf(evidence);
    }

    /** The next version of this detection: closed at {@code endedAt}, with more evidence added. */
    public Detection closed(Instant endedAt, Map<String, Object> moreEvidence) {
        Map<String, Object> all = new HashMap<>(evidence);
        all.putAll(moreEvidence);
        return new Detection(detectionId, type, DetectionStatus.CLOSED, version + 1, mmsi, otherMmsi, startedAt,
                Objects.requireNonNull(endedAt, "endedAt"), latitude, longitude, h3Cell, rulesVersion, all);
    }
}
