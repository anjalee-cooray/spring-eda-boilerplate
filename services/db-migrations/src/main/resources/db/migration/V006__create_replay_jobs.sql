-- Replay jobs table
-- Tracks event replay requests — operator triggers a replay, this table records
-- scope, progress, and outcome. The replay service queries outbox_records (PUBLISHED)
-- and re-publishes matching events back to the broker.
--
-- Why replay is safe:
--   Events are re-published with their original event_id. Each consumer's
--   inbox_records table deduplicates by event_id — consumers that already
--   processed the event skip it silently. New or reset consumers process normally.
CREATE TABLE replay_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT        NOT NULL,
    event_type      TEXT,                       -- null = all event types
    from_timestamp  TIMESTAMPTZ,                -- null = beginning of time
    to_timestamp    TIMESTAMPTZ,                -- null = job creation time
    specific_ids    TEXT,                       -- comma-separated outbox record UUIDs, null = query-based
    status          TEXT        NOT NULL DEFAULT 'PENDING',  -- PENDING | RUNNING | COMPLETED | FAILED
    total_events    INTEGER,                    -- populated when RUNNING starts
    replayed_count  INTEGER     NOT NULL DEFAULT 0,
    error_message   TEXT,
    requested_by    TEXT        NOT NULL,       -- operator identifier (username, system, etc.)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);

CREATE INDEX idx_replay_jobs_tenant_id  ON replay_jobs (tenant_id);
CREATE INDEX idx_replay_jobs_status     ON replay_jobs (status);
CREATE INDEX idx_replay_jobs_created_at ON replay_jobs (created_at DESC);
