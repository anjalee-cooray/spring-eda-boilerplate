package com.example.eda.gateway.quota;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Enforces per-tenant daily request quotas using Redis counters.
 *
 * Quota enforcement flow:
 *   1. Read tenant_id from the X-Tenant-Id header (set by TenantContextGatewayFilter)
 *   2. Increment the daily counter in Redis (INCR quota:{tenant}:daily:{date})
 *   3. Compare counter to the tenant's limit stored in quota:{tenant}:limit:daily
 *   4. Hard limit exceeded → 429 Too Many Requests
 *   5. Soft limit (>=80%) → forward with X-Quota-Warning header
 *   6. No limit configured → forward without enforcement (unknown tenant, no quota)
 *
 * Counter key: quota:{tenant_id}:daily:requests:{YYYY-MM-DD}
 *   Expires after 25 hours so counters auto-clean and reset at midnight UTC.
 *
 * Limit key: quota:{tenant_id}:limit:daily
 *   Set by operators via the quota admin API or:
 *     redis-cli SET quota:tenant-1:limit:daily 1000
 *
 * Soft limit key: quota:{tenant_id}:limit:daily:soft-pct  (optional, default 80)
 *   redis-cli SET quota:tenant-1:limit:daily:soft-pct 90
 */
@Component
public class TenantQuotaFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(TenantQuotaFilter.class);

    static final String COUNTER_KEY_PREFIX = "quota";
    static final String LIMIT_KEY_SUFFIX   = "limit:daily";
    static final String SOFT_PCT_KEY_SUFFIX = "limit:daily:soft-pct";
    static final int    DEFAULT_SOFT_PCT   = 80;
    static final Duration COUNTER_TTL      = Duration.ofHours(25);

    private final ReactiveStringRedisTemplate redis;
    private final Counter rejectedCounter;

    public TenantQuotaFilter(ReactiveStringRedisTemplate redis, MeterRegistry meterRegistry) {
        this.redis = redis;
        this.rejectedCounter = Counter.builder("tenant.quota.rejected")
                .description("Requests rejected due to tenant daily quota exhaustion")
                .register(meterRegistry);
    }

    @Override
    public int getOrder() {
        // After TenantContextGatewayFilter (+1) and IdempotencyFilter (+2), before routing
        return Ordered.HIGHEST_PRECEDENCE + 3;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String tenantId = exchange.getRequest().getHeaders().getFirst("X-Tenant-Id");
        if (tenantId == null || tenantId.isBlank()) {
            // No tenant context — let security layer handle it
            return chain.filter(exchange);
        }

        String today       = LocalDate.now(ZoneOffset.UTC).toString();
        String counterKey  = COUNTER_KEY_PREFIX + ":" + tenantId + ":daily:requests:" + today;
        String limitKey    = COUNTER_KEY_PREFIX + ":" + tenantId + ":" + LIMIT_KEY_SUFFIX;
        String softPctKey  = COUNTER_KEY_PREFIX + ":" + tenantId + ":" + SOFT_PCT_KEY_SUFFIX;

        return redis.opsForValue().increment(counterKey)
                .flatMap(count -> {
                    // Set TTL on first increment
                    if (count == 1L) {
                        return redis.expire(counterKey, COUNTER_TTL).thenReturn(count);
                    }
                    return Mono.just(count);
                })
                .flatMap(count -> redis.opsForValue().get(limitKey)
                        .flatMap(limitStr -> {
                            long limit = Long.parseLong(limitStr);

                            if (count > limit) {
                                log.warn("Tenant quota exceeded tenantId={} count={} limit={}", tenantId, count, limit);
                                rejectedCounter.increment();
                                return rejectOverQuota(exchange, tenantId, count, limit);
                            }

                            return redis.opsForValue().get(softPctKey)
                                    .map(pctStr -> {
                                        try { return Integer.parseInt(pctStr); }
                                        catch (NumberFormatException e) { return DEFAULT_SOFT_PCT; }
                                    })
                                    .defaultIfEmpty(DEFAULT_SOFT_PCT)
                                    .flatMap(softPct -> {
                                        long softThreshold = limit * softPct / 100L;
                                        if (count >= softThreshold) {
                                            log.debug("Tenant quota soft limit approached tenantId={} count={}/{}", tenantId, count, limit);
                                            exchange.getResponse().getHeaders()
                                                    .add("X-Quota-Warning", "Daily request quota at " + (count * 100 / limit) + "%");
                                        }
                                        addRemainingHeader(exchange, count, limit);
                                        return chain.filter(exchange);
                                    });
                        })
                        // No limit configured — pass through without enforcement
                        .switchIfEmpty(chain.filter(exchange))
                );
    }

    private Mono<Void> rejectOverQuota(ServerWebExchange exchange, String tenantId, long count, long limit) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        response.getHeaders().add("Content-Type", "application/json");
        response.getHeaders().add("X-Quota-Limit", String.valueOf(limit));
        response.getHeaders().add("X-Quota-Used", String.valueOf(count));
        response.getHeaders().add("Retry-After", "86400"); // try again tomorrow

        byte[] body = """
                {"error":"quota_exceeded",\
                "message":"Daily request quota exhausted. Upgrade your plan or retry tomorrow.",\
                "quotaUsed":%d,"quotaLimit":%d}\
                """.formatted(count, limit).getBytes(StandardCharsets.UTF_8);

        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }

    private void addRemainingHeader(ServerWebExchange exchange, long count, long limit) {
        long remaining = Math.max(0L, limit - count);
        exchange.getResponse().getHeaders().add("X-Quota-Remaining", String.valueOf(remaining));
        exchange.getResponse().getHeaders().add("X-Quota-Limit", String.valueOf(limit));
    }
}
