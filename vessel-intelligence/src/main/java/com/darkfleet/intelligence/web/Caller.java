package com.darkfleet.intelligence.web;

import com.darkfleet.security.Roles;
import org.springframework.security.core.Authentication;

/** Who is calling, as recorded in proposals, decisions and the audit log. */
public record Caller(String name, boolean admin) {

    public static Caller of(Authentication authentication) {
        boolean admin = authentication.getAuthorities().stream()
                .anyMatch(a -> ("ROLE_" + Roles.ADMIN).equals(a.getAuthority()));
        return new Caller(authentication.getName(), admin);
    }
}
