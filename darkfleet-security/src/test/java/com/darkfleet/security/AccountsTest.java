package com.darkfleet.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

class AccountsTest {

    @Test
    void anAccountWithoutPasswordStopsTheStart() {
        DarkFleetSecurityProperties.UserAccount account = new DarkFleetSecurityProperties.UserAccount();
        account.setName("analyst");
        account.setPassword("");
        account.setRoles(List.of(Roles.ANALYST));
        DarkFleetSecurityProperties properties = new DarkFleetSecurityProperties();
        properties.setUsers(List.of(account));

        assertThrows(IllegalStateException.class, () -> new DarkFleetSecurityAutoConfiguration()
                .userDetailsService(properties, new BCryptPasswordEncoder()));
    }
}
