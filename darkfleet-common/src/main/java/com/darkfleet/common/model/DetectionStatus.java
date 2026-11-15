package com.darkfleet.common.model;

public enum DetectionStatus {
    /** Still going on: the vessel is still silent, still loitering, the vessels are still together. */
    OPEN,
    /** Over; {@code endedAt} says when and the evidence says why it closed. */
    CLOSED
}
