package com.example.eda.security;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantContextHolderTest {

    @AfterEach
    void clear() {
        TenantContextHolder.clear();
    }

    @Test
    void setsAndGetsContext() {
        TenantContext ctx = new TenantContext("tenant-1", "user-1", List.of());
        TenantContextHolder.set(ctx);
        assertThat(TenantContextHolder.get()).isSameAs(ctx);
    }

    @Test
    void throwsWhenNoContextBound() {
        assertThatThrownBy(TenantContextHolder::get)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No TenantContext");
    }

    @Test
    void getOrNullReturnsNullWhenNotSet() {
        assertThat(TenantContextHolder.getOrNull()).isNull();
    }

    @Test
    void clearRemovesContext() {
        TenantContextHolder.set(new TenantContext("tenant-1", "user-1", List.of()));
        TenantContextHolder.clear();
        assertThat(TenantContextHolder.getOrNull()).isNull();
    }
}
