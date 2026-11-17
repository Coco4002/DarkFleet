package com.darkfleet.intelligence.events;

/**
 * Something analysts watching the live stream should see: a new or updated detection, a new proposal,
 * a decision. Published after the change is committed.
 *
 * @param name the SSE event name, e.g. {@code detection}, {@code proposal}
 * @param data sent as JSON
 */
public record AnalystEvent(String name, Object data) {
}
