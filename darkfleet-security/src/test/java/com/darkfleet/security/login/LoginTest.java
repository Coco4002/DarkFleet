package com.darkfleet.security.login;

import com.darkfleet.security.ApiKeyAuthenticationFilter;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.cookie;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(
        classes = LoginTest.TestApp.class,
        properties = {
                "darkfleet.security.login-enabled=true",
                "darkfleet.security.users[0].name=ana",
                "darkfleet.security.users[0].password=ana-pass",
                "darkfleet.security.users[0].roles=ANALYST",
                "darkfleet.security.users[1].name=root",
                "darkfleet.security.users[1].password=root-pass",
                "darkfleet.security.users[1].roles=ADMIN,ANALYST",
                "darkfleet.security.users[2].name=unset",
                "darkfleet.security.users[2].password=",
                "darkfleet.security.api-clients[0].name=internal",
                "darkfleet.security.api-clients[0].key=service-key"
        })
@AutoConfigureMockMvc
class LoginTest {

    @Autowired
    MockMvc mvc;

    private static String body(String user, String password) {
        return "{\"username\":\"" + user + "\",\"password\":\"" + password + "\"}";
    }

    /** Fetches the real XSRF-TOKEN cookie the browser gets on its first GET. */
    private Cookie csrfCookie() throws Exception {
        MvcResult result = mvc.perform(get("/api/v1/auth/me"))
                .andExpect(status().isOk())
                .andExpect(cookie().exists("XSRF-TOKEN"))
                .andReturn();
        Cookie xsrf = result.getResponse().getCookie("XSRF-TOKEN");
        assertNotNull(xsrf);
        return xsrf;
    }

    private MockHttpSession login(String user, String password) throws Exception {
        Cookie xsrf = csrfCookie();
        MvcResult result = mvc.perform(post("/api/v1/auth/login")
                        .cookie(xsrf)
                        .header("X-XSRF-TOKEN", xsrf.getValue())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(user, password)))
                .andExpect(status().isOk())
                .andReturn();
        return (MockHttpSession) result.getRequest().getSession(false);
    }

    @Test
    void anonymousVisitorIsToldSoAndGetsACsrfCookie() throws Exception {
        mvc.perform(get("/api/v1/auth/me"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.authenticated").value(false))
                .andExpect(cookie().exists("XSRF-TOKEN"));
    }

    @Test
    void protectedPathsStillNeedLogin() throws Exception {
        mvc.perform(get("/secure/ping")).andExpect(status().isUnauthorized());
    }

    @Test
    void wrongPasswordOrUnsetAccountIsRefused() throws Exception {
        Cookie xsrf = csrfCookie();
        mvc.perform(post("/api/v1/auth/login").cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue())
                .contentType(MediaType.APPLICATION_JSON).content(body("ana", "nope"))).andExpect(status().isUnauthorized());
        mvc.perform(post("/api/v1/auth/login").cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue())
                .contentType(MediaType.APPLICATION_JSON).content(body("nobody", "x"))).andExpect(status().isUnauthorized());
        mvc.perform(post("/api/v1/auth/login").cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue())
                .contentType(MediaType.APPLICATION_JSON).content(body("unset", ""))).andExpect(status().isBadRequest());
    }

    @Test
    void loginOpensASessionWithTheAccountsRoles() throws Exception {
        MockHttpSession session = login("root", "root-pass");
        mvc.perform(get("/secure/ping").session(session)).andExpect(status().isOk());
        mvc.perform(get("/api/v1/auth/me").session(session))
                .andExpect(jsonPath("$.authenticated").value(true))
                .andExpect(jsonPath("$.username").value("root"))
                .andExpect(jsonPath("$.roles", hasItem("ADMIN")));
    }

    @Test
    void sessionWritesNeedTheCsrfToken() throws Exception {
        MockHttpSession session = login("root", "root-pass");
        Cookie xsrf = csrfCookie();
        mvc.perform(post("/admin/ping").session(session)).andExpect(status().isForbidden());
        mvc.perform(post("/admin/ping").session(session).cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue()))
                .andExpect(status().isOk());
    }

    @Test
    void rolesAreStillCheckedForPeople() throws Exception {
        MockHttpSession session = login("ana", "ana-pass");
        Cookie xsrf = csrfCookie();
        mvc.perform(get("/secure/ping").session(session)).andExpect(status().isOk());
        mvc.perform(post("/admin/ping").session(session).cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue()))
                .andExpect(status().isForbidden());
    }

    @Test
    void serviceKeysNeedNoCsrfToken() throws Exception {
        mvc.perform(get("/secure/ping").header(ApiKeyAuthenticationFilter.HEADER, "service-key"))
                .andExpect(status().isOk());
        mvc.perform(post("/service/ping").header(ApiKeyAuthenticationFilter.HEADER, "service-key"))
                .andExpect(status().isOk());
    }

    @Test
    void aWrongKeyIsStillRefused() throws Exception {
        mvc.perform(post("/service/ping").header(ApiKeyAuthenticationFilter.HEADER, "wrong"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void logoutEndsTheSession() throws Exception {
        MockHttpSession session = login("ana", "ana-pass");
        Cookie xsrf = csrfCookie();
        mvc.perform(post("/api/v1/auth/logout").session(session).cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue()))
                .andExpect(status().isNoContent());
        mvc.perform(get("/secure/ping").session(session)).andExpect(status().isUnauthorized());
    }

    @Test
    void sessionIdChangesOnLogin() throws Exception {
        Cookie xsrf = csrfCookie();
        MockHttpSession planted = new MockHttpSession();
        String before = planted.getId();
        MvcResult result = mvc.perform(post("/api/v1/auth/login").session(planted)
                        .cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue())
                        .contentType(MediaType.APPLICATION_JSON).content(body("ana", "ana-pass")))
                .andExpect(status().isOk()).andReturn();
        assertNotEquals(before, result.getRequest().getSession().getId());
    }

    @Test
    void repeatedWrongPasswordsLockTheAccountEvenForTheRightOne() throws Exception {
        Cookie xsrf = csrfCookie();
        for (int i = 0; i < 5; i++) {
            mvc.perform(post("/api/v1/auth/login").cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue())
                    .contentType(MediaType.APPLICATION_JSON).content(body("lockme", "wrong")))
                    .andExpect(status().isUnauthorized());
        }
        mvc.perform(post("/api/v1/auth/login").cookie(xsrf).header("X-XSRF-TOKEN", xsrf.getValue())
                .contentType(MediaType.APPLICATION_JSON).content(body("lockme", "wrong")))
                .andExpect(status().isTooManyRequests());
    }

    @SpringBootApplication
    static class TestApp {

        @RestController
        static class TestController {

            @GetMapping("/secure/ping")
            String securePing() {
                return "ok";
            }

            @PostMapping("/admin/ping")
            @PreAuthorize("hasRole('ADMIN')")
            String adminPing() {
                return "ok";
            }

            @PostMapping("/service/ping")
            @PreAuthorize("hasRole('SERVICE')")
            String servicePing() {
                return "ok";
            }
        }
    }
}
