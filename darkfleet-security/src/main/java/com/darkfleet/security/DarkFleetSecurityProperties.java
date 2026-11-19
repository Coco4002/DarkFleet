package com.darkfleet.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code darkfleet.security.*}: who may call a service and which paths are open to everyone.
 * Clients with a blank key are ignored, so an unset environment variable never creates a usable key.
 */
@ConfigurationProperties(prefix = "darkfleet.security")
public class DarkFleetSecurityProperties {

    /** Callers authenticated by the X-Api-Key header: other services and the admin. */
    private List<ApiClient> apiClients = new ArrayList<>();

    /** Paths open to everyone for any HTTP method (Ant-style patterns). */
    private List<String> publicPaths = new ArrayList<>();

    /** Paths open to everyone for GET only. */
    private List<String> publicGetPaths = new ArrayList<>();

    /** Turns on browser login (session cookie + CSRF protection) for the people in {@link #users}. */
    private boolean loginEnabled = false;

    /** People who may log in. Accounts with a blank name or password are ignored. */
    private List<UserAccount> users = new ArrayList<>();

    public boolean isLoginEnabled() {
        return loginEnabled;
    }

    public void setLoginEnabled(boolean loginEnabled) {
        this.loginEnabled = loginEnabled;
    }

    public List<UserAccount> getUsers() {
        return users;
    }

    public void setUsers(List<UserAccount> users) {
        this.users = users;
    }

    public static class UserAccount {
        private String name;
        /** Plain text from the environment; hashed with bcrypt when the service starts, never kept as given. */
        private String password;
        private List<String> roles = new ArrayList<>(List.of(Roles.ANALYST));

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public List<String> getRoles() {
            return roles;
        }

        public void setRoles(List<String> roles) {
            this.roles = roles;
        }
    }

    public List<ApiClient> getApiClients() {
        return apiClients;
    }

    public void setApiClients(List<ApiClient> apiClients) {
        this.apiClients = apiClients;
    }

    public List<String> getPublicPaths() {
        return publicPaths;
    }

    public void setPublicPaths(List<String> publicPaths) {
        this.publicPaths = publicPaths;
    }

    public List<String> getPublicGetPaths() {
        return publicGetPaths;
    }

    public void setPublicGetPaths(List<String> publicGetPaths) {
        this.publicGetPaths = publicGetPaths;
    }

    public static class ApiClient {
        private String name;
        private String key;
        private List<String> roles = new ArrayList<>(List.of(Roles.SERVICE));

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public List<String> getRoles() {
            return roles;
        }

        public void setRoles(List<String> roles) {
            this.roles = roles;
        }
    }
}
