package com.darkfleet.security;

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

    /** No password accounts yet; this also stops Spring Boot from generating a default user. */
    @Bean
    @ConditionalOnMissingBean
    public UserDetailsService userDetailsService() {
        return new InMemoryUserDetailsManager();
    }

    @Bean
    @ConditionalOnMissingBean(SecurityFilterChain.class)
    public SecurityFilterChain darkFleetSecurityFilterChain(HttpSecurity http, DarkFleetSecurityProperties properties)
            throws Exception {
        http
                // Safe only because no credential is sent automatically by browsers (see class comment).
                .csrf(csrf -> csrf.disable())
                .httpBasic(basic -> basic.disable())
                .formLogin(form -> form.disable())
                .logout(logout -> logout.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .exceptionHandling(e -> e.authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)))
                .addFilterBefore(new ApiKeyAuthenticationFilter(properties.getApiClients()),
                        BasicAuthenticationFilter.class)
                .authorizeHttpRequests(auth -> {
                    auth.requestMatchers(ALWAYS_PUBLIC).permitAll();
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
