-- Transactional Outbox table
-- Written atomically with domain writes; polled by outbox-relay to publish events to the broker
CREATE TABLE outbox_records (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT NOT NULL,
    event_id        UUID NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    correlation_id  TEXT,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX idx_outbox_records_status_created ON outbox_records (status, created_at)
    WHERE status = 'PENDING';

CREATE INDEX idx_outbox_records_tenant_id ON outbox_records (tenant_id);
CREATE INDEX idx_outbox_records_event_id ON outbox_records (event_id);
