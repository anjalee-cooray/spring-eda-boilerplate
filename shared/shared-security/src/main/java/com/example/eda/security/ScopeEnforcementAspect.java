package com.example.eda.security;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * AOP aspect that enforces OAuth2 scopes declared via @RequiredScope.
 *
 * Execution order:
 *   1. Spring Security authenticates the JWT and sets SecurityContext.
 *   2. @PreAuthorize checks roles (RBAC).
 *   3. This aspect (highest precedence among application aspects) checks scopes (ABAC).
 *   4. Controller method executes.
 *
 * Scope extraction:
 *   Scopes are read from the JWT {@code scope} claim (space-separated string) or
 *   {@code scp} claim (list). Both formats are handled — Okta uses {@code scp},
 *   Auth0 uses {@code scope}, Azure AD uses both.
 *
 * Method-level @RequiredScope takes precedence over class-level @RequiredScope.
 * If neither is present on the target method or its declaring class, this aspect
 * is a no-op for that invocation.
 */
@Aspect
@Component
public class ScopeEnforcementAspect {

    private static final Logger log = LoggerFactory.getLogger(ScopeEnforcementAspect.class);

    @Around("@within(com.example.eda.security.RequiredScope) || @annotation(com.example.eda.security.RequiredScope)")
    public Object enforceScope(ProceedingJoinPoint pjp) throws Throwable {
        RequiredScope annotation = resolveAnnotation(pjp);
        if (annotation == null) {
            return pjp.proceed();
        }

        List<String> tokenScopes = extractScopes();
        String[] required = annotation.value();

        boolean hasScope = Arrays.stream(required).anyMatch(tokenScopes::contains);
        if (!hasScope) {
            log.warn("Scope check failed method={} required={} tokenScopes={}",
                    pjp.getSignature().toShortString(), Arrays.toString(required), tokenScopes);
            throw new InsufficientScopeException(required);
        }

        return pjp.proceed();
    }

    private RequiredScope resolveAnnotation(ProceedingJoinPoint pjp) {
        MethodSignature sig = (MethodSignature) pjp.getSignature();
        Method method = sig.getMethod();

        // Method-level takes precedence
        RequiredScope methodAnnotation = method.getAnnotation(RequiredScope.class);
        if (methodAnnotation != null) return methodAnnotation;

        // Fall back to class-level
        return method.getDeclaringClass().getAnnotation(RequiredScope.class);
    }

    @SuppressWarnings("unchecked")
    private List<String> extractScopes() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !(auth.getPrincipal() instanceof Jwt jwt)) {
            return Collections.emptyList();
        }

        // Try "scope" claim (space-separated string — Auth0, Azure AD)
        String scopeStr = jwt.getClaimAsString("scope");
        if (scopeStr != null && !scopeStr.isBlank()) {
            return Arrays.asList(scopeStr.split(" "));
        }

        // Try "scp" claim (list — Okta, Microsoft)
        Object scp = jwt.getClaim("scp");
        if (scp instanceof List<?>) {
            return (List<String>) scp;
        }

        return Collections.emptyList();
    }
}
