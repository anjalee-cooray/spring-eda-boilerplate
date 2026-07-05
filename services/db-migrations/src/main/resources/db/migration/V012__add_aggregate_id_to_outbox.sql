-- Partition key for per-entity Kafka message ordering.
--
-- The outbox relay sends "tenantId:aggregateId" as the Kafka message key, so all
-- events for the same entity (Order, Booking, Account, etc.) land on the same
-- partition and are consumed in FIFO order by all downstream consumers.
--
-- NULL means tenant-level routing (all events for the tenant share a partition).
-- This is the backward-compatible default for events written before this column existed.
ALTER TABLE outbox_records ADD COLUMN aggregate_id TEXT;

-- Partial index — only index non-null values (sparse) to keep index size small.
CREATE INDEX idx_outbox_records_aggregate_id
    ON outbox_records (aggregate_id)
    WHERE aggregate_id IS NOT NULL;
