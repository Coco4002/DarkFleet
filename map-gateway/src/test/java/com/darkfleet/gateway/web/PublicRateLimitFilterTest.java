package com.darkfleet.gateway.web;

import com.darkfleet.gateway.GatewayProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublicRateLimitFilterTest {

    @Test
    void enforcesPerIpRateLimitAndRefillsOverTime() throws Exception {
        Instant t0 = Instant.parse("2026-11-20T12:00:00Z");
        GatewayProperties props = new GatewayProperties(false, Duration.ofHours(1), Duration.ofDays(7),
                Duration.ofDays(7), Duration.ofSeconds(5), Duration.ofMinutes(2), "http://localhost:8086",
                "test-key", Duration.ofSeconds(5), "http://localhost/w/api.php", Duration.ofSeconds(3),
                Duration.ofHours(24), 3, 2);
        PublicRateLimitFilter filter = new PublicRateLimitFilter(props, Clock.fixed(t0, ZoneOffset.UTC));

        for (int i = 0; i < 3; i++) {
            // an encoded path counts like any other
            MockHttpServletRequest req = new MockHttpServletRequest("GET", i == 0 ? "/api/v1/m%61p/snapshot" : "/api/v1/map/snapshot");
            req.addHeader("X-Forwarded-For", "203.0.113.10");
            MockHttpServletResponse res = new MockHttpServletResponse();
            filter.doFilter(req, res, new MockFilterChain());
            assertEquals(200, res.getStatus());
        }

        // 4th request in the same instant from 203.0.113.10 is rate-limited with 429 and Retry-After
        MockHttpServletRequest blockedReq = new MockHttpServletRequest("GET", "/api/v1/map/snapshot");
        blockedReq.addHeader("X-Forwarded-For", "203.0.113.10");
        MockHttpServletResponse blockedRes = new MockHttpServletResponse();
        filter.doFilter(blockedReq, blockedRes, new MockFilterChain());
        assertEquals(429, blockedRes.getStatus());
        assertEquals("5", blockedRes.getHeader("Retry-After"));
        assertTrue(blockedRes.getContentAsString().contains("Rate limit exceeded"));

        // Another IP is unaffected
        MockHttpServletRequest otherIpReq = new MockHttpServletRequest("GET", "/api/v1/map/snapshot");
        otherIpReq.addHeader("X-Forwarded-For", "198.51.100.7");
        MockHttpServletResponse otherIpRes = new MockHttpServletResponse();
        filter.doFilter(otherIpReq, otherIpRes, new MockFilterChain());
        assertEquals(200, otherIpRes.getStatus());
    }
}
