package com.example.eda.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.slf4j.MDC;
import org.springframework.lang.NonNull;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Extracts tenant_id and roles from the validated JWT and populates TenantContextHolder.
 * Runs after Spring Security's JWT authentication filter.
 *
 * Okta claim mapping:
 *   tenant_id  — custom claim added via Okta Expression Language in the token inline hook
 *   roles      — custom claim mapped from Okta groups (configured in Okta admin console)
 *
 * For local dev, mock-oauth2-server injects these claims via its JSON_CONFIG.
 */
public class TenantContextFilter extends OncePerRequestFilter {

    static final String TENANT_ID_CLAIM = "tenant_id";
    static final String ROLES_CLAIM = "roles";

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain chain) throws ServletException, IOException {

        try {
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            if (auth != null && auth.getPrincipal() instanceof Jwt jwt) {
                String tenantId = jwt.getClaimAsString(TENANT_ID_CLAIM);
                String subject = jwt.getSubject();
                List<String> roles = extractRoles(jwt);

                if (tenantId != null && !tenantId.isBlank()) {
                    TenantContext ctx = new TenantContext(tenantId, subject, roles);
                    TenantContextHolder.set(ctx);
                    MDC.put("tenant_id", tenantId);
                }
            }
            chain.doFilter(request, response);
        } finally {
            TenantContextHolder.clear();
            MDC.remove("tenant_id");
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> extractRoles(Jwt jwt) {
        Object claim = jwt.getClaims().get(ROLES_CLAIM);
        if (claim instanceof List<?> list) {
            return list.stream()
                    .filter(String.class::isInstance)
                    .map(String.class::cast)
                    .toList();
        }
        return List.of();
    }
}
