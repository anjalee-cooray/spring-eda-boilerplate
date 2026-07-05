package com.example.eda.gateway.quota;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Manages per-tenant daily quota limits stored in Redis.
 *
 * Only PLATFORM_OPERATOR may modify quotas.
 *
 * Quota keys in Redis:
 *   quota:{tenant_id}:limit:daily          — max requests per day
 *   quota:{tenant_id}:limit:daily:soft-pct — soft-warning threshold (0-100)
 *   quota:{tenant_id}:daily:requests:{date} — live request counter (read-only via GET)
 *
 * Preset helper:
 *   PUT /admin/quotas/{tenantId}/tier/{tier}  — apply FREE/PRO/ENTERPRISE preset
 */
@RestController
@RequestMapping("/admin/quotas")
@PreAuthorize("hasRole('PLATFORM_OPERATOR')")
public class TenantQuotaAdminController {

    private static final Logger log = LoggerFactory.getLogger(TenantQuotaAdminController.class);

    private static final long FREE_DAILY_LIMIT       = 1_000L;
    private static final long PRO_DAILY_LIMIT        = 10_000L;
    private static final long ENTERPRISE_DAILY_LIMIT = 1_000_000_000L;

    private final ReactiveStringRedisTemplate redis;

    public TenantQuotaAdminController(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    @GetMapping("/{tenantId}")
    public Mono<ResponseEntity<QuotaStatus>> getQuota(@PathVariable String tenantId) {
        String limitKey   = limitKey(tenantId);
        String counterKey = counterKey(tenantId);

        return Mono.zip(
                redis.opsForValue().get(limitKey).defaultIfEmpty("none"),
                redis.opsForValue().get(counterKey).defaultIfEmpty("0")
        ).map(tuple -> ResponseEntity.ok(new QuotaStatus(
                tenantId,
                tuple.getT1(),
                Long.parseLong(tuple.getT2())
        )));
    }

    @PutMapping("/{tenantId}/daily-limit")
    public Mono<ResponseEntity<String>> setDailyLimit(
            @PathVariable String tenantId,
            @RequestBody SetLimitRequest request) {
        if (request.limit() < 1) {
            return Mono.just(ResponseEntity.badRequest().body("limit must be >= 1"));
        }
        String key = limitKey(tenantId);
        return redis.opsForValue().set(key, String.valueOf(request.limit()))
                .doOnSuccess(v -> log.info("Quota set tenantId={} dailyLimit={}", tenantId, request.limit()))
                .thenReturn(ResponseEntity.ok("Daily limit set to " + request.limit()));
    }

    @PutMapping("/{tenantId}/tier/{tier}")
    public Mono<ResponseEntity<String>> applyTierPreset(
            @PathVariable String tenantId,
            @PathVariable String tier) {
        long limit = switch (tier.toUpperCase()) {
            case "FREE"       -> FREE_DAILY_LIMIT;
            case "PRO"        -> PRO_DAILY_LIMIT;
            case "ENTERPRISE" -> ENTERPRISE_DAILY_LIMIT;
            default -> throw new IllegalArgumentException("Unknown tier: " + tier + ". Valid: FREE, PRO, ENTERPRISE");
        };
        return redis.opsForValue().set(limitKey(tenantId), String.valueOf(limit))
                .doOnSuccess(v -> log.info("Quota preset applied tenantId={} tier={} limit={}", tenantId, tier, limit))
                .thenReturn(ResponseEntity.ok("Tier " + tier + " applied: daily limit = " + limit));
    }

    @DeleteMapping("/{tenantId}")
    public Mono<ResponseEntity<String>> removeQuota(@PathVariable String tenantId) {
        return redis.delete(limitKey(tenantId))
                .doOnSuccess(n -> log.info("Quota removed tenantId={}", tenantId))
                .thenReturn(ResponseEntity.ok("Quota removed for " + tenantId));
    }

    private String limitKey(String tenantId) {
        return TenantQuotaFilter.COUNTER_KEY_PREFIX + ":" + tenantId + ":" + TenantQuotaFilter.LIMIT_KEY_SUFFIX;
    }

    private String counterKey(String tenantId) {
        String today = LocalDate.now(ZoneOffset.UTC).toString();
        return TenantQuotaFilter.COUNTER_KEY_PREFIX + ":" + tenantId + ":daily:requests:" + today;
    }

    record SetLimitRequest(long limit) {}

    record QuotaStatus(String tenantId, String dailyLimit, long todayCount) {}
}
