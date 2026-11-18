package com.darkfleet.intelligence.api;

/**
 * Who may call what. Analysts and calling services (the MCP server) read; the public map gateway
 * reads only the track and coverage endpoints it passes through; analysts decide; only an
 * administrator changes detection rules or imports data. A service can propose but never approve.
 */
final class Access {

    static final String READ = "hasAnyRole('ANALYST', 'SERVICE')";
    static final String GATEWAY = "hasAnyRole('ANALYST', 'SERVICE', 'GATEWAY')";
    static final String PROPOSE = "hasAnyRole('ANALYST', 'SERVICE')";
    static final String DECIDE = "hasRole('ANALYST')";
    static final String ADMIN = "hasRole('ADMIN')";

    private Access() {
    }
}
