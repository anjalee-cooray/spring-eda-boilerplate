-- Add schema_version to outbox_records to preserve the payload schema version used
-- at publish time. Required for safe event replay — the relay re-publishes with the
-- original schema_version so consumers can apply the correct upcaster chain.
--
-- DEFAULT '1' ensures backward compatibility: rows inserted before this migration
-- (where the column did not exist) are treated as schema version 1.

ALTER TABLE outbox_records
    ADD COLUMN schema_version TEXT NOT NULL DEFAULT '1';
