package com.example.eda.db.quota;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Per-tenant request and event quotas enforced by the API gateway.
 *
 * dailyRequestLimit   — maximum HTTP requests per calendar day (UTC).
 *                       Counted in Redis with a key that expires at midnight UTC.
 * monthlyEventLimit   — maximum domain events per calendar month (UTC).
 *                       Counted in Redis, reset on the 1st of each month.
 * quotaSoftLimitPct   — percentage at which X-Quota-Warning header is added.
 * hardLimitEnforced   — if false, quota violations log/warn but do not reject (grace mode).
 *
 * Tier presets (operators choose when onboarding a tenant):
 *   FREE       — 1 000 req/day,  10 000 events/month
 *   PRO        — 10 000 req/day, 100 000 events/month
 *   ENTERPRISE — effectively unlimited (1 000 000 000)
 */
@Entity
@Table(name = "tenant_quotas")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TenantQuota {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "tenant_id", nullable = false, unique = true)
    private String tenantId;

    @Column(name = "tier", nullable = false)
    @Builder.Default
    private String tier = "FREE";

    @Column(name = "daily_request_limit", nullable = false)
    @Builder.Default
    private int dailyRequestLimit = 1_000;

    @Column(name = "monthly_event_limit", nullable = false)
    @Builder.Default
    private int monthlyEventLimit = 10_000;

    @Column(name = "quota_soft_limit_pct", nullable = false)
    @Builder.Default
    private int quotaSoftLimitPct = 80;

    @Column(name = "hard_limit_enforced", nullable = false)
    @Builder.Default
    private boolean hardLimitEnforced = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    /** Returns true if the given count exceeds the daily request limit. */
    public boolean isDailyRequestExceeded(long currentCount) {
        return currentCount > dailyRequestLimit;
    }

    /** Returns true if the current count has crossed the soft-warning threshold. */
    public boolean isDailyRequestNearLimit(long currentCount) {
        return currentCount >= (dailyRequestLimit * quotaSoftLimitPct / 100L);
    }
}
