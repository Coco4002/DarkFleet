package com.darkfleet.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

/**
 * Authenticates service-to-service calls carrying an {@code X-Api-Key} header.
 * A request with a wrong key is rejected outright instead of falling through to other
 * authentication, so a typo in a service's key fails loudly.
 */
public class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Api-Key";

    private final List<DarkFleetSecurityProperties.ApiClient> clients;

    public ApiKeyAuthenticationFilter(List<DarkFleetSecurityProperties.ApiClient> clients) {
        this.clients = clients.stream()
                .filter(c -> c.getKey() != null && !c.getKey().isBlank())
                .toList();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String presented = request.getHeader(HEADER);
        if (presented == null || presented.isBlank()) {
            chain.doFilter(request, response);
            return;
        }
        byte[] presentedBytes = presented.getBytes(StandardCharsets.UTF_8);
        DarkFleetSecurityProperties.ApiClient match = null;
        for (DarkFleetSecurityProperties.ApiClient client : clients) {
            // Constant-time comparison; keep checking all clients so timing doesn't reveal which matched.
            if (MessageDigest.isEqual(presentedBytes, client.getKey().getBytes(StandardCharsets.UTF_8))) {
                match = client;
            }
        }
        if (match == null) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid API key");
            return;
        }
        var authorities = match.getRoles().stream()
                .map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                .toList();
        var authentication = UsernamePasswordAuthenticationToken.authenticated(match.getName(), null, authorities);
        var context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(authentication);
        SecurityContextHolder.setContext(context);
        try {
            chain.doFilter(request, response);
        } finally {
            SecurityContextHolder.clearContext();
        }
    }
}
