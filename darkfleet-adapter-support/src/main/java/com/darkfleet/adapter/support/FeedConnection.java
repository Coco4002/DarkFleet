package com.darkfleet.adapter.support;

/**
 * Connection state of a live feed, reported in its health heartbeats.
 */
public interface FeedConnection {

    boolean isConnected();

    /** Reconnects since the adapter started (never reset). */
    long totalReconnects();
}
