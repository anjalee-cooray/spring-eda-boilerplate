-- Cold-storage table for old event_store records.
--
-- EventStoreArchivalJob moves rows from event_store to this table when they
-- are older than app.event-store.archive-after-days (default 365 days).
--
-- Design decisions:
--   1. Same schema as event_store — no information is lost on archival.
--   2. Separate table (not a partition) so primary event_store stays hot and
--      fast for recent-event queries (index scans stay small).
--   3. archived_at timestamp documents when the row moved, enabling audit.
--   4. event_store_archive is NOT subject to RLS (archived data is read by
--      platform operators, not tenants). Restrict access at the DB role level.
--
-- Recovery:
--   If an archived event needs to be replayed, copy it back to event_store or
--   query event_store_archive directly in a custom ReplayJobService path.

CREATE TABLE event_store_archive (
    LIKE event_store INCLUDING ALL,    -- copies columns, constraints, defaults
    archived_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Efficient access by tenant + time range (same pattern as event_store)
CREATE INDEX idx_event_store_archive_tenant_occurred
    ON event_store_archive (tenant_id, occurred_at DESC);

COMMENT ON TABLE event_store_archive IS
    'Cold storage for event_store records older than app.event-store.archive-after-days';
