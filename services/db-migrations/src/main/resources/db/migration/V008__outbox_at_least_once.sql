-- Adds columns required for the claim-before-publish pattern that gives
-- at-least-once delivery guarantees under relay failure and horizontal scaling.
--
-- locked_until  : timestamp until which this record is held by a relay instance.
--                 NULL when status = PENDING or PUBLISHED.
--                 Set to now() + lock_timeout when status = IN_FLIGHT.
--                 The OutboxReclaimTask resets expired IN_FLIGHT rows to PENDING.
--
-- attempt_count : incremented at claim time (not at failure time) so that a relay
--                 crash between claim and complete still counts as an attempt.
--                 When attempt_count reaches max_attempts the record moves to FAILED.
--
-- last_error    : last exception message recorded on publish failure.
--                 Cleared on successful publish.

ALTER TABLE outbox_records
    ADD COLUMN locked_until   TIMESTAMPTZ,
    ADD COLUMN attempt_count  INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN last_error     TEXT;

-- Fast lookup for the reclaimer: expired IN_FLIGHT records
CREATE INDEX idx_outbox_inflight_locked ON outbox_records (locked_until)
    WHERE status = 'IN_FLIGHT';

-- Replace the old PENDING index with a composite that covers the claim query
DROP INDEX IF EXISTS idx_outbox_records_status_created;
CREATE INDEX idx_outbox_pending_claim ON outbox_records (created_at)
    WHERE status = 'PENDING';

-- Alert / monitoring index: find FAILED records quickly
CREATE INDEX idx_outbox_failed ON outbox_records (created_at)
    WHERE status = 'FAILED';
