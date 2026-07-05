package com.example.eda.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the OAuth2 scopes required to invoke a method or controller.
 *
 * Use alongside @PreAuthorize to enforce both RBAC (roles) and ABAC (scopes):
 *
 * <pre>{@code
 * @PostMapping
 * @PreAuthorize("hasRole('TENANT_ADMIN')")
 * @RequiredScope("commands:write")
 * public ResponseEntity<...> create(...) { ... }
 *
 * @GetMapping
 * @PreAuthorize("hasAnyRole('TENANT_ADMIN', 'TENANT_MEMBER')")
 * @RequiredScope("queries:read")
 * public ResponseEntity<...> list(...) { ... }
 * }</pre>
 *
 * Scopes are extracted from the JWT {@code scope} or {@code scp} claim (space-separated).
 * Example token scopes: "openid profile commands:write queries:read admin:manage"
 *
 * If the JWT is missing the required scope, ScopeEnforcementAspect throws
 * InsufficientScopeException which maps to HTTP 403.
 *
 * Configure scopes in your OIDC provider (e.g. Okta):
 *   - commands:write — for POST/PUT/DELETE to command routes
 *   - queries:read  — for GET to query routes
 *   - admin:manage  — for platform operator endpoints (replay, rebuild, quota)
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiredScope {

    /** One or more OAuth2 scopes required (ANY match — client needs at least one). */
    String[] value();
}
