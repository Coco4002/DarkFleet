package com.darkfleet.intelligence.api;

/**
 * Who may call what. Analysts and calling services (the MCP server) read; analysts decide; only an
 * administrator changes detection rules or imports data. A service can propose but never approve.
 */
final class Access {

    static final String READ = "hasAnyRole('ANALYST', 'SERVICE')";
    static final String PROPOSE = "hasAnyRole('ANALYST', 'SERVICE')";
    static final String DECIDE = "hasRole('ANALYST')";
    static final String ADMIN = "hasRole('ADMIN')";

    private Access() {
    }
}
