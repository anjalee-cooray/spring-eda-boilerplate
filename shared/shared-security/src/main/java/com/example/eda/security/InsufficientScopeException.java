package com.example.eda.security;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Thrown by ScopeEnforcementAspect when the JWT is missing a required OAuth2 scope.
 * Maps to HTTP 403 Forbidden — the token is valid but does not grant access.
 */
@ResponseStatus(HttpStatus.FORBIDDEN)
public class InsufficientScopeException extends RuntimeException {

    private final String[] requiredScopes;

    public InsufficientScopeException(String[] requiredScopes) {
        super("JWT is missing required scope(s): " + String.join(", ", requiredScopes));
        this.requiredScopes = requiredScopes;
    }

    public String[] getRequiredScopes() { return requiredScopes; }
}
