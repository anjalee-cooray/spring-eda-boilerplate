-- Enable Row-Level Security on all tenant-scoped tables
-- The application sets app.tenant_id via SET LOCAL before every query (see RlsDataSourceInterceptor)

ALTER TABLE outbox_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbox_records ENABLE ROW LEVEL SECURITY;

-- Safe default: if no tenant context is set, return zero rows (never all rows)
CREATE POLICY tenant_isolation_outbox ON outbox_records
    USING (tenant_id = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation_inbox ON inbox_records
    USING (tenant_id = current_setting('app.tenant_id', true));

-- Platform operator role bypasses RLS for cross-tenant operations
-- Grant this role only to the operator service and migration runner
CREATE ROLE platform_operator;
ALTER TABLE outbox_records FORCE ROW LEVEL SECURITY;
ALTER TABLE inbox_records FORCE ROW LEVEL SECURITY;

-- Application role — used by all services; subject to RLS
CREATE ROLE app_user;
GRANT SELECT, INSERT, UPDATE ON outbox_records TO app_user;
GRANT SELECT, INSERT ON inbox_records TO app_user;
GRANT SELECT, INSERT, UPDATE ON tenants TO app_user;

GRANT BYPASS ROW LEVEL SECURITY ON TABLE outbox_records TO platform_operator;
GRANT BYPASS ROW LEVEL SECURITY ON TABLE inbox_records TO platform_operator;
