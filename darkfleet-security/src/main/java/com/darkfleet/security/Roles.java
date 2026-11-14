package com.darkfleet.security;

/**
 * Roles used across DarkFleet. Anonymous visitors have no role.
 */
public final class Roles {

    /** Analyst: reviews detections, cases and agent proposals. */
    public static final String ANALYST = "ANALYST";

    /** Administrator: analyst rights plus rule changes and account management. */
    public static final String ADMIN = "ADMIN";

    /** Another DarkFleet service calling with an API key (e.g. the MCP server). */
    public static final String SERVICE = "SERVICE";

    private Roles() {
    }
}
