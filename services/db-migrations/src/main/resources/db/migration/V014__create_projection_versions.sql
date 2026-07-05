-- Tracks the version and health of each CQRS read-side projection.
-- When a projection's logic changes (bug fix, schema migration), bump version
-- and trigger a rebuild: the projector truncates its read-model table and
-- re-streams events from event_store via the EVENT_STORE replay path.
--
-- Lifecycle: CURRENT → REBUILDING → CURRENT (success) | FAILED (error)
CREATE TABLE projection_versions (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    projection_name TEXT        NOT NULL UNIQUE,    -- e.g. "ExampleProjector"
    current_version INTEGER     NOT NULL DEFAULT 1,
    status          TEXT        NOT NULL DEFAULT 'CURRENT', -- CURRENT | REBUILDING | FAILED
    last_rebuilt_at TIMESTAMPTZ,
    rebuild_job_id  UUID,           -- fk to replay_jobs.id for the active/last rebuild
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_projection_versions_name ON projection_versions (projection_name);

-- Seed known projections so operators can query status immediately.
INSERT INTO projection_versions (projection_name, current_version, status)
VALUES ('ExampleProjector', 1, 'CURRENT');
