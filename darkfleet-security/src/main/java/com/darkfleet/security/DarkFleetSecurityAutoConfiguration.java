package com.darkfleet.security;

import jakarta.servlet.DispatcherType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.csrf.CsrfTokenRequestAttributeHandler;

import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

/**
 * Shared security for every DarkFleet servlet service.
 * <ul>
 *   <li>Health probes and OpenAPI docs are open; so are the paths a service lists as public.</li>
 *   <li>Everything else needs an {@code X-Api-Key} header; the key's roles are checked per
 *       endpoint with {@code @PreAuthorize}.</li>
 * </ul>
 * There are deliberately no browser-held credentials (no HTTP Basic, no cookies): a browser never
 * attaches the API key header on its own, so a cross-site request cannot act with someone's rights
 * and CSRF tokens are not needed. Human login for the web app (session + CSRF protection) is added
 * with the web client.
 */
@AutoConfiguration(beforeName = {
        "org.springframework.boot.security.autoconfigure.UserDetailsServiceAutoConfiguration",
        "org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration",
        "org.springframework.boot.security.autoconfigure.actuate.web.servlet.ManagementWebSecurityAutoConfiguration"
})
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableConfigurationProperties(DarkFleetSecurityProperties.class)
@EnableWebSecurity
@EnableMethodSecurity
public class DarkFleetSecurityAutoConfiguration {

    static final String[] ALWAYS_PUBLIC = {
            "/actuator/health", "/actuator/health/**",
            "/v3/api-docs", "/v3/api-docs/**", "/swagger-ui.html", "/swagger-ui/**",
            "/error"
    };

    /** Accounts from {@code darkfleet.security.users}; none by default, which also stops Boot generating a default user. */
    @Bean
    @ConditionalOnMissingBean
    public UserDetailsService userDetailsService(DarkFleetSecurityProperties properties, PasswordEncoder encoder) {
        var accounts = properties.getUsers().stream()
                .filter(u -> u.getName() != null && !u.getName().isBlank()
                        && u.getPassword() != null && !u.getPassword().isEmpty())
                .map(u -> User.withUsername(u.getName().trim())
                        .password(encoder.encode(u.getPassword()))
                        .roles(u.getRoles().toArray(String[]::new))
                        .build())
                .toList();
        return new InMemoryUserDetailsManager(accounts);
    }

    @Bean
    @ConditionalOnMissingBean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "darkfleet.security.login-enabled", havingValue = "true")
    public AuthController authController(UserDetailsService users, PasswordEncoder encoder) {
        DaoAuthenticationProvider provider = new DaoAuthenticationProvider(users);
        provider.setPasswordEncoder(encoder);
        return new AuthController(new ProviderManager(provider), new LoginThrottle(Clock.systemUTC()));
    }

    @Bean
    @ConditionalOnMissingBean(SecurityFilterChain.class)
    public SecurityFilterChain darkFleetSecurityFilterChain(HttpSecurity http, DarkFleetSecurityProperties properties)
            throws Exception {
        boolean login = properties.isLoginEnabled();
        http
                .httpBasic(basic -> basic.disable())
                .formLogin(form -> form.disable())
                .logout(logout -> logout.disable())
                .exceptionHandling(e -> e.authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)))
                .addFilterBefore(new ApiKeyAuthenticationFilter(properties.getApiClients()),
                        BasicAuthenticationFilter.class);
        if (login) {
            // People log in with a session cookie, so every state-changing request needs the CSRF token.
            // Calls carrying an API key are exempt: a browser never adds that header by itself.
            CsrfTokenRequestAttributeHandler handler = new CsrfTokenRequestAttributeHandler();
            handler.setCsrfRequestAttributeName(null); // plain token, read straight from the header
            http
                    .csrf(csrf -> csrf
                            .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
                            .csrfTokenRequestHandler(handler)
                            .requireCsrfProtectionMatcher(request -> {
                                String method = request.getMethod();
                                boolean safe = "GET".equals(method) || "HEAD".equals(method)
                                        || "OPTIONS".equals(method) || "TRACE".equals(method);
                                return !safe && request.getHeader(ApiKeyAuthenticationFilter.HEADER) == null;
                            }))
                    .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED))
                    .addFilterAfter(new CsrfCookieFilter(), BasicAuthenticationFilter.class);
        } else {
            // Safe only because no credential is sent automatically by browsers (see class comment).
            http
                    .csrf(csrf -> csrf.disable())
                    .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
        }
        http
                .authorizeHttpRequests(auth -> {
                    // The original request was already authorized. Its async dispatch (e.g. when a
                    // server-sent event stream ends) and error dispatch carry no API key of their own.
                    auth.dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll();
                    auth.requestMatchers(ALWAYS_PUBLIC).permitAll();
                    if (login) {
                        auth.requestMatchers(HttpMethod.POST, "/api/v1/auth/login", "/api/v1/auth/logout").permitAll();
                        auth.requestMatchers(HttpMethod.GET, "/api/v1/auth/me").permitAll();
                    }
                    if (!properties.getPublicPaths().isEmpty()) {
                        auth.requestMatchers(properties.getPublicPaths().toArray(String[]::new)).permitAll();
                    }
                    if (!properties.getPublicGetPaths().isEmpty()) {
                        auth.requestMatchers(HttpMethod.GET, properties.getPublicGetPaths().toArray(String[]::new))
                                .permitAll();
                    }
                    auth.anyRequest().authenticated();
                });
        return http.build();
    }
}
