package com.darkfleet.security;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(
        classes = DarkFleetSecurityAutoConfigurationTest.TestApp.class,
        properties = {
                "darkfleet.security.api-clients[0].name=admin",
                "darkfleet.security.api-clients[0].key=admin-key",
                "darkfleet.security.api-clients[0].roles=ADMIN,ANALYST",
                "darkfleet.security.api-clients[1].name=analyst",
                "darkfleet.security.api-clients[1].key=analyst-key",
                "darkfleet.security.api-clients[1].roles=ANALYST",
                "darkfleet.security.api-clients[2].name=mcp-server",
                "darkfleet.security.api-clients[2].key=service-key",
                "darkfleet.security.api-clients[3].name=unset",
                "darkfleet.security.api-clients[3].key=",
                "darkfleet.security.public-get-paths=/public/**"
        })
@AutoConfigureMockMvc
class DarkFleetSecurityAutoConfigurationTest {

    private static final String KEY = ApiKeyAuthenticationFilter.HEADER;

    @Autowired
    MockMvc mvc;

    @Test
    void healthAndPublicGetAreOpenToEveryone() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
        mvc.perform(get("/public/ping")).andExpect(status().isOk());
    }

    @Test
    void publicGetPathsDoNotOpenOtherMethods() throws Exception {
        mvc.perform(post("/public/ping")).andExpect(status().isUnauthorized());
    }

    @Test
    void protectedPathsNeedAKey() throws Exception {
        mvc.perform(get("/secure/ping")).andExpect(status().isUnauthorized());
    }

    @Test
    void rolesAreEnforcedPerEndpoint() throws Exception {
        mvc.perform(get("/secure/ping").header(KEY, "analyst-key")).andExpect(status().isOk());
        mvc.perform(post("/admin/ping").header(KEY, "analyst-key")).andExpect(status().isForbidden());
        mvc.perform(post("/admin/ping").header(KEY, "admin-key")).andExpect(status().isOk());
        mvc.perform(get("/service/ping").header(KEY, "service-key")).andExpect(status().isOk());
        mvc.perform(post("/admin/ping").header(KEY, "service-key")).andExpect(status().isForbidden());
    }

    @Test
    void wrongOrBlankKeyIsRejected() throws Exception {
        mvc.perform(get("/secure/ping").header(KEY, "wrong-key")).andExpect(status().isUnauthorized());
        mvc.perform(get("/secure/ping").header(KEY, " ")).andExpect(status().isUnauthorized());
    }

    /**
     * Regression for CSRF: browsers can resend cached HTTP Basic credentials or cookies on a
     * cross-site form post. Neither may authenticate; only the explicit header does.
     */
    @Test
    void browserHeldCredentialsNeverAuthenticate() throws Exception {
        String basic = "Basic " + Base64.getEncoder().encodeToString("admin:admin-key".getBytes(StandardCharsets.UTF_8));
        mvc.perform(post("/admin/ping").header(HttpHeaders.AUTHORIZATION, basic))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/admin/ping")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .header(HttpHeaders.ORIGIN, "https://evil.example")
                        .cookie(new jakarta.servlet.http.Cookie("JSESSIONID", "stolen")))
                .andExpect(status().isUnauthorized());
    }

    @SpringBootApplication
    static class TestApp {

        @RestController
        static class TestController {

            @GetMapping("/public/ping")
            String publicPing() {
                return "ok";
            }

            @PostMapping("/public/ping")
            String publicPost() {
                return "ok";
            }

            @GetMapping("/secure/ping")
            String securePing() {
                return "ok";
            }

            @PostMapping("/admin/ping")
            @PreAuthorize("hasRole('ADMIN')")
            String adminPing() {
                return "ok";
            }

            @GetMapping("/service/ping")
            @PreAuthorize("hasRole('SERVICE')")
            String servicePing() {
                return "ok";
            }
        }
    }
}
