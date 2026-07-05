-- Per-consumer health tracking table.
--
-- Stores the last successfully processed event for each (consumer, event_type, tenant)
-- combination. Updated after every successful handle() call.
--
-- Used for:
--   - Lag detection: if last_event_at is old, the consumer is stuck or behind
--   - Processing rate: events_processed counter per consumer
--   - Health endpoint: "last event processed N seconds ago"
--   - Alert: consumer hasn't processed an event in >threshold seconds
--
-- consumer_name maps to the Java class simple name (e.g. "ExampleCreatedHandler").
-- event_type is the event this row tracks (allows per-type lag per consumer).
-- tenant_id scopes to a specific tenant for multi-tenant monitoring.

CREATE TABLE consumer_offsets (
    consumer_name     TEXT        NOT NULL,
    event_type        TEXT        NOT NULL,
    tenant_id         TEXT        NOT NULL,
    last_event_id     UUID,
    last_event_at     TIMESTAMPTZ,
    events_processed  BIGINT      NOT NULL DEFAULT 0,
    last_error        TEXT,
    last_error_at     TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_type, tenant_id)
);

-- Health dashboard: all consumers ordered by staleness
CREATE INDEX idx_consumer_offsets_last_event ON consumer_offsets (last_event_at ASC);
