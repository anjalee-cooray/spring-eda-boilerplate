-- Per-tenant request and event quotas.
-- The API gateway's TenantQuotaFilter reads from Redis (fast path) and falls
-- back to this table for quota limits. Redis counters are incremented per
-- request and reset on the daily/monthly boundary stored here.
--
-- Enforcement:
--   daily_request_limit   — max HTTP requests per calendar day (UTC)
--   monthly_event_limit   — max domain events published per calendar month (UTC)
--
-- Soft vs hard:
--   quota_soft_limit_pct  — percentage at which a warning header is added (X-Quota-Warning)
--   hard_limit_enforced   — if false, quotas log/warn but do not reject requests (grace mode)

CREATE TABLE tenant_quotas (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               TEXT        NOT NULL UNIQUE,
    tier                    TEXT        NOT NULL DEFAULT 'FREE',   -- FREE | PRO | ENTERPRISE
    daily_request_limit     INTEGER     NOT NULL DEFAULT 1000,
    monthly_event_limit     INTEGER     NOT NULL DEFAULT 10000,
    quota_soft_limit_pct    INTEGER     NOT NULL DEFAULT 80,       -- warn at 80%
    hard_limit_enforced     BOOLEAN     NOT NULL DEFAULT true,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_tenant_quotas_tenant_id ON tenant_quotas (tenant_id);

-- Default quotas per tier (operators insert tenant rows as tenants onboard).
-- Free tier: 1 000 req/day, 10 000 events/month
-- Pro tier:  10 000 req/day, 100 000 events/month
-- Enterprise: unlimited (1 000 000 000 effectively)
COMMENT ON TABLE tenant_quotas IS
    'Per-tenant request and event quotas enforced at the API gateway and outbox relay';
