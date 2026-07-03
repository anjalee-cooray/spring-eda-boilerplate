package com.example.eda.security;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantContextTest {

    @Test
    void constructsWithValidFields() {
        TenantContext ctx = new TenantContext("tenant-1", "user-1", List.of(Roles.TENANT_ADMIN));
        assertThat(ctx.tenantId()).isEqualTo("tenant-1");
        assertThat(ctx.subject()).isEqualTo("user-1");
        assertThat(ctx.roles()).containsExactly(Roles.TENANT_ADMIN);
    }

    @Test
    void throwsWhenTenantIdIsBlank() {
        assertThatThrownBy(() -> new TenantContext("", "user-1", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tenantId");
    }

    @Test
    void throwsWhenTenantIdIsNull() {
        assertThatThrownBy(() -> new TenantContext(null, "user-1", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void hasRoleReturnsTrueForMatchingRole() {
        TenantContext ctx = new TenantContext("tenant-1", "user-1", List.of(Roles.TENANT_ADMIN));
        assertThat(ctx.hasRole(Roles.TENANT_ADMIN)).isTrue();
        assertThat(ctx.hasRole(Roles.TENANT_MEMBER)).isFalse();
    }

    @Test
    void rolesDefaultToEmptyListWhenNull() {
        TenantContext ctx = new TenantContext("tenant-1", "user-1", null);
        assertThat(ctx.roles()).isEmpty();
    }
}
