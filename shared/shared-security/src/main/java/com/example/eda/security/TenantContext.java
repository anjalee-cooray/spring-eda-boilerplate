package com.example.eda.security;

import java.util.Collections;
import java.util.List;

/**
 * Immutable tenant context extracted from a validated JWT.
 */
public record TenantContext(String tenantId, String subject, List<String> roles) {

    public TenantContext {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        roles = roles == null ? Collections.emptyList() : Collections.unmodifiableList(roles);
    }

    public boolean hasRole(String role) {
        return roles.contains(role);
    }
}
