package com.example.eda.gateway.ratelimit;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * Rate limiter key resolver — scopes rate limits per tenant.
 *
 * The X-Tenant-Id header is set upstream by TenantContextGatewayFilter
 * (which extracts it from the validated JWT) so it is always present for
 * authenticated requests.
 *
 * To implement per-tier limits (Free=10 req/s, Pro=100 req/s, Enterprise=unlimited),
 * replace the static replenishRate/burstCapacity values in application.yml with a
 * custom RateLimiter bean that looks up the tenant's tier from a cache or DB and
 * returns tier-specific limits:
 *
 *   @Bean
 *   RateLimiter tierAwareRateLimiter(TenantTierRepository tiers) {
 *       return new TierAwareRateLimiter(tiers);
 *   }
 *
 * deny-empty-key=true (default) — if X-Tenant-Id is missing the request is
 * rejected with 403 before hitting downstream. This should not happen in practice
 * because unauthenticated requests are rejected by security before this filter runs.
 */
@Configuration
public class RateLimiterConfig {

    @Bean
    public KeyResolver tenantKeyResolver() {
        return exchange -> {
            String tenantId = exchange.getRequest().getHeaders().getFirst("X-Tenant-Id");
            return Mono.justOrEmpty(tenantId).filter(id -> !id.isBlank());
        };
    }
}
