package com.darkfleet.security;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextHolderStrategy;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Login for people (analysts and admins) of the web client: username and password in, a session cookie out.
 * The cookie is HttpOnly and SameSite=Strict, and every state-changing request must also carry the
 * CSRF token (the {@code XSRF-TOKEN} cookie echoed in an {@code X-XSRF-TOKEN} header). Repeated wrong
 * passwords lock the account for a short while.
 */
@RestController
@RequestMapping("/api/v1/auth")
@ConditionalOnProperty(name = "darkfleet.security.login-enabled", havingValue = "true")
public class AuthController {

    public record LoginRequest(String username, String password) {
    }

    public record SessionView(boolean authenticated, String username, List<String> roles) {
    }

    private final AuthenticationManager authenticationManager;
    private final LoginThrottle throttle;
    private final SecurityContextRepository repository = new HttpSessionSecurityContextRepository();
    private final SecurityContextHolderStrategy holder = SecurityContextHolder.getContextHolderStrategy();

    public AuthController(AuthenticationManager authenticationManager, LoginThrottle throttle) {
        this.authenticationManager = authenticationManager;
        this.throttle = throttle;
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody LoginRequest body, HttpServletRequest request,
                                   HttpServletResponse response) {
        if (body == null || body.username() == null || body.password() == null
                || body.username().isBlank() || body.password().isEmpty()) {
            return problem(HttpStatus.BAD_REQUEST, "Username and password are required");
        }
        String user = body.username().trim();
        if (throttle.locked(user)) {
            return problem(HttpStatus.TOO_MANY_REQUESTS, "Too many failed attempts; try again in a minute");
        }
        Authentication authentication;
        try {
            authentication = authenticationManager.authenticate(
                    UsernamePasswordAuthenticationToken.unauthenticated(user, body.password()));
        } catch (AuthenticationException e) {
            throttle.failed(user);
            return problem(HttpStatus.UNAUTHORIZED, "Wrong username or password");
        }
        throttle.succeeded(user);
        // a new session id on every login: a session id planted before login is worthless
        if (request.getSession(false) != null) {
            request.changeSessionId();
        }
        request.getSession(true);
        SecurityContext context = holder.createEmptyContext();
        context.setAuthentication(authentication);
        holder.setContext(context);
        repository.saveContext(context, request, response);
        return ResponseEntity.ok(view(authentication));
    }

    @GetMapping("/me")
    public SessionView me(Authentication authentication) {
        if (authentication == null || authentication instanceof AnonymousAuthenticationToken
                || !authentication.isAuthenticated()) {
            return new SessionView(false, null, List.of());
        }
        return view(authentication);
    }

    @PostMapping("/logout")
    public ResponseEntity<Void> logout(HttpServletRequest request) {
        var session = request.getSession(false);
        if (session != null) {
            session.invalidate();
        }
        SecurityContextHolder.clearContext();
        return ResponseEntity.noContent().build();
    }

    private static SessionView view(Authentication authentication) {
        List<String> roles = authentication.getAuthorities().stream()
                .map(a -> a.getAuthority().replaceFirst("^ROLE_", ""))
                .sorted()
                .toList();
        return new SessionView(true, authentication.getName(), roles);
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String detail) {
        return ResponseEntity.status(status).body(ProblemDetail.forStatusAndDetail(status, detail));
    }
}
