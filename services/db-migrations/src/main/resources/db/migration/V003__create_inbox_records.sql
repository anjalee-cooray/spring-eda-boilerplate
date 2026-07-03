-- Inbox deduplication table
-- Consumers insert a row here after processing an event; checked before processing to ensure idempotency
CREATE TABLE inbox_records (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_inbox_records_event_id ON inbox_records (event_id);
CREATE INDEX idx_inbox_records_tenant_id ON inbox_records (tenant_id);

-- Purge records older than 30 days to prevent unbounded table growth
-- Wire this to a scheduled job in production
CREATE INDEX idx_inbox_records_processed_at ON inbox_records (processed_at);
