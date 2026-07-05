-- Add replay_source column to replay_jobs.
-- OUTBOX  (default) — replay from outbox_records (PUBLISHED rows); bounded by broker retention.
-- EVENT_STORE       — replay from event_store (permanent log); used for read-model rebuild.
ALTER TABLE replay_jobs
    ADD COLUMN replay_source TEXT NOT NULL DEFAULT 'OUTBOX';

COMMENT ON COLUMN replay_jobs.replay_source IS
    'OUTBOX = re-publish from outbox_records; EVENT_STORE = re-publish from event_store';
