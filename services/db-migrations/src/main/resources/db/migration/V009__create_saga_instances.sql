-- Saga instance table for the DB-backed orchestrator.
--
-- Each row is one running instance of a multi-step saga. The orchestrator stores
-- all mutable saga state here so it survives relay/consumer crashes and restarts.
--
-- correlation_id links every event emitted or consumed within a single saga run.
-- The orchestrator finds its own instance by looking up correlation_id on incoming
-- response events — consumers must echo back the saga's correlation_id.
--
-- context (JSONB) holds saga-type-specific fields (e.g. booking_id, amount) so
-- they are available at every step and compensation step without reloading from
-- multiple tables.

CREATE TABLE saga_instances (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_type               TEXT NOT NULL,
    correlation_id          TEXT NOT NULL UNIQUE,
    status                  TEXT NOT NULL DEFAULT 'STARTED',
    current_step            TEXT NOT NULL,
    context                 JSONB NOT NULL DEFAULT '{}',
    compensation_step_index INTEGER NOT NULL DEFAULT 0,
    failure_reason          TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at            TIMESTAMPTZ
);

CREATE INDEX idx_saga_correlation_id ON saga_instances (correlation_id);
CREATE INDEX idx_saga_status         ON saga_instances (status) WHERE status IN ('STARTED','RUNNING','COMPENSATING');
