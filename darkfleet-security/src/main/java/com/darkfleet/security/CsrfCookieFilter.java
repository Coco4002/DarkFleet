package com.darkfleet.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Reading the CSRF token makes Spring Security send it as the {@code XSRF-TOKEN} cookie. Without this,
 * the cookie would only appear on requests that happen to ask for the token, and the web client would
 * have none to echo back.
 */
final class CsrfCookieFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Object token = request.getAttribute(CsrfToken.class.getName());
        if (token == null) {
            token = request.getAttribute("_csrf");
        }
        if (token instanceof CsrfToken csrf) {
            csrf.getToken();
        }
        chain.doFilter(request, response);
    }
}
