-- Permanent, append-only event log.
--
-- Unlike outbox_records (which are mutable — status changes from PENDING → PUBLISHED),
-- event_store rows are written once and never updated or deleted. This gives you:
--
--   1. Full audit trail  — every event ever emitted, timestamped and tenant-scoped
--   2. Event sourcing    — reconstruct any aggregate by replaying its events
--   3. Durable replay   — independent of broker retention (Kafka log compaction,
--                         SQS message expiry); the event store is the source of truth
--
-- Written in the same transaction as outbox_records (via EventStoreWriter, called
-- inside OutboxWriter). Atomicity guarantee: an event either appears in both tables
-- or neither. The outbox relay reads from outbox_records; the event store is read-only
-- for replay and audit queries.
--
-- aggregate_id and aggregate_type are optional fields for event-sourcing use cases
-- where events belong to a specific aggregate (e.g. appointment_id, order_id).
-- Leave them NULL for system-level events that don't belong to one entity.

CREATE TABLE event_store (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    aggregate_id    TEXT,
    aggregate_type  TEXT,
    schema_version  TEXT NOT NULL DEFAULT '1',
    payload         JSONB NOT NULL,
    correlation_id  TEXT,
    causation_id    TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The primary replay/audit access pattern: all events for a tenant, newest first
CREATE INDEX idx_event_store_tenant_occurred   ON event_store (tenant_id, occurred_at DESC);

-- Event sourcing: all events for a specific aggregate in order
CREATE INDEX idx_event_store_aggregate         ON event_store (aggregate_id, occurred_at ASC)
    WHERE aggregate_id IS NOT NULL;

-- Audit: find events by type within a time range
CREATE INDEX idx_event_store_type_occurred     ON event_store (event_type, occurred_at DESC);

-- Deduplication guard (event_id UNIQUE index already created above by the UNIQUE constraint)
