package com.example.eda.db.quota;

/**
 * Thrown by TenantQuotaEnforcer when a tenant's daily request limit is exceeded.
 * Caught at the controller layer and mapped to HTTP 429.
 */
public class QuotaExceededException extends RuntimeException {

    private final String tenantId;
    private final long current;
    private final long limit;

    public QuotaExceededException(String tenantId, long current, long limit) {
        super("Tenant %s exceeded daily quota: %d / %d".formatted(tenantId, current, limit));
        this.tenantId = tenantId;
        this.current  = current;
        this.limit    = limit;
    }

    public String getTenantId() { return tenantId; }
    public long getCurrent()    { return current; }
    public long getLimit()      { return limit; }
}
