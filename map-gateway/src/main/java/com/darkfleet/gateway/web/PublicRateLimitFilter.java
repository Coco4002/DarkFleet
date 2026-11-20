package com.darkfleet.gateway.web;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.gateway.GatewayProperties;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;

/**
 * Per-client-IP token-bucket rate limiter on the public map API ({@code /api/v1/map/**}).
 * Protects {@code map-gateway} and the upstream {@code vessel-intelligence} read endpoints from
 * scraping or request floods once the map is open to anonymous visitors.
 */
@Component
public class PublicRateLimitFilter extends OncePerRequestFilter {

    private static final int MAX_TRACKED_IPS = 20_000;
    private static final Duration BUCKET_EXPIRY = Duration.ofMinutes(5);

    private final int capacity;
    private final double refillPerNano;
    private final Clock clock;
    private final Cache<String, Bucket> buckets;

    public PublicRateLimitFilter(GatewayProperties properties, Clock clock) {
        this.capacity = properties.rateLimitPerMinute();
        this.refillPerNano = (double) capacity / Duration.ofMinutes(1).toNanos();
        this.clock = clock;
        this.buckets = Caffeine.newBuilder()
                .expireAfterAccess(BUCKET_EXPIRY)
                .maximumSize(MAX_TRACKED_IPS)
                .build();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path == null || !path.startsWith("/api/v1/map/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String ip = clientIp(request);
        Bucket bucket = buckets.get(ip, k -> new Bucket(capacity, clock.instant().getEpochSecond(),
                clock.instant().getNano()));
        if (!bucket.tryConsume(clock, capacity, refillPerNano)) {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            response.setHeader("Retry-After", "5");
            response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
            ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                    HttpStatus.TOO_MANY_REQUESTS, "Rate limit exceeded; please slow down");
            problem.setInstance(URI.create(request.getRequestURI()));
            JsonSerde.mapper().writeValue(response.getOutputStream(), problem);
            return;
        }
        chain.doFilter(request, response);
    }

    public static String clientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            int comma = forwarded.indexOf(',');
            String first = (comma >= 0 ? forwarded.substring(0, comma) : forwarded).trim();
            if (!first.isEmpty()) {
                return first;
            }
        }
        String remote = request.getRemoteAddr();
        return remote == null || remote.isBlank() ? "unknown" : remote;
    }

    private static final class Bucket {
        private double tokens;
        private long lastSec;
        private int lastNano;

        Bucket(double tokens, long lastSec, int lastNano) {
            this.tokens = tokens;
            this.lastSec = lastSec;
            this.lastNano = lastNano;
        }

        synchronized boolean tryConsume(Clock clock, int capacity, double refillPerNano) {
            var now = clock.instant();
            long elapsedNanos = (now.getEpochSecond() - lastSec) * 1_000_000_000L + (now.getNano() - lastNano);
            if (elapsedNanos > 0) {
                tokens = Math.min(capacity, tokens + elapsedNanos * refillPerNano);
                lastSec = now.getEpochSecond();
                lastNano = now.getNano();
            }
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;
            }
            return false;
        }
    }
}
