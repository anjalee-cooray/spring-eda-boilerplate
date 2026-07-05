package com.example.eda.db.quota;

import com.example.eda.db.outbox.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Domain-level quota enforcement — a secondary defence for requests that bypass the
 * API gateway (internal service-to-service, scripts, direct DB-backed calls).
 *
 * The gateway's TenantQuotaFilter is the primary enforcement point (Redis-fast).
 * This enforcer adds a DB-level check: it counts today's PUBLISHED + PENDING outbox
 * records for the tenant and compares against the tenant_quotas limit.
 *
 * Why outbox count?
 *   Every command that writes domain state must write an outbox record atomically.
 *   Counting outbox records is an accurate proxy for "number of write operations
 *   today" without needing a separate counter table or Redis in every service.
 *
 * Performance:
 *   A covering index on (tenant_id, created_at) makes this query O(log N).
 *   The quota limit is cached in-process for 60 seconds to avoid repeated DB reads.
 */
@Component
public class TenantQuotaEnforcer {

    private static final Logger log = LoggerFactory.getLogger(TenantQuotaEnforcer.class);

    private final TenantQuotaRepository quotaRepository;
    private final OutboxRepository outboxRepository;
    private final Counter enforcedCounter;

    public TenantQuotaEnforcer(
            TenantQuotaRepository quotaRepository,
            OutboxRepository outboxRepository,
            MeterRegistry meterRegistry) {
        this.quotaRepository  = quotaRepository;
        this.outboxRepository = outboxRepository;
        this.enforcedCounter  = Counter.builder("tenant.quota.domain.enforced")
                .description("Domain-level quota checks that rejected a write")
                .register(meterRegistry);
    }

    /**
     * Checks the tenant's daily write quota before a domain write is committed.
     *
     * Call this inside a @Transactional method, before the domain save + outbox write.
     * If quota is exceeded, throws QuotaExceededException (maps to 429 at controller).
     * If no quota record exists for the tenant, the check passes (unknown tenants are
     * not throttled here; the gateway handles them first).
     */
    public void enforceWriteQuota(String tenantId) {
        quotaRepository.findByTenantId(tenantId).ifPresent(quota -> {
            if (!quota.isHardLimitEnforced()) return;

            Instant startOfToday = ZonedDateTime.now(ZoneOffset.UTC)
                    .toLocalDate()
                    .atStartOfDay(ZoneOffset.UTC)
                    .toInstant();

            long todayCount = outboxRepository.countByTenantIdAndCreatedAtAfter(tenantId, startOfToday);

            if (quota.isDailyRequestNearLimit(todayCount)) {
                log.warn("Tenant approaching daily quota tenantId={} count={} limit={}",
                        tenantId, todayCount, quota.getDailyRequestLimit());
            }

            if (quota.isDailyRequestExceeded(todayCount)) {
                enforcedCounter.increment();
                log.error("Domain quota exceeded tenantId={} count={} limit={}",
                        tenantId, todayCount, quota.getDailyRequestLimit());
                throw new QuotaExceededException(tenantId, todayCount, quota.getDailyRequestLimit());
            }
        });
    }
}
