package com.darkfleet.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

class AccountsTest {

    @Test
    void anAccountWithoutPasswordOrRoleStopsTheStart() {
        assertRefused(account("analyst", "", List.of(Roles.ANALYST)));
        assertRefused(account("analyst", "   ", List.of(Roles.ANALYST)));
        assertRefused(account("analyst", "secret", List.of(" ")));
    }

    private static DarkFleetSecurityProperties.UserAccount account(String name, String password, List<String> roles) {
        DarkFleetSecurityProperties.UserAccount account = new DarkFleetSecurityProperties.UserAccount();
        account.setName(name);
        account.setPassword(password);
        account.setRoles(roles);
        return account;
    }

    private static void assertRefused(DarkFleetSecurityProperties.UserAccount account) {
        DarkFleetSecurityProperties properties = new DarkFleetSecurityProperties();
        properties.setUsers(List.of(account));
        assertThrows(IllegalStateException.class, () -> new DarkFleetSecurityAutoConfiguration()
                .userDetailsService(properties, new BCryptPasswordEncoder()));
    }
}
