package com.example.eda.relay.replay;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Parameters for a replay job.
 *
 * All fields except tenantId and requestedBy are optional.
 * Filters combine with AND — e.g. eventType + fromTimestamp replays only
 * that event type within the date range.
 *
 * Examples:
 *
 *   Full tenant replay (rebuild all read models from scratch):
 *     { tenantId: "t1", requestedBy: "ops" }
 *
 *   Replay a specific event type (e.g. after fixing a handler bug):
 *     { tenantId: "t1", eventType: "example.created", requestedBy: "ops" }
 *
 *   Replay within a date range (e.g. events lost during an outage window):
 *     { tenantId: "t1", fromTimestamp: "2025-01-01T00:00:00Z",
 *       toTimestamp: "2025-01-02T00:00:00Z", requestedBy: "ops" }
 *
 *   Replay specific outbox records by ID:
 *     { tenantId: "t1", specificIds: ["uuid1", "uuid2"], requestedBy: "ops" }
 *
 *   Read-model rebuild from permanent event log (not bounded by broker retention):
 *     { tenantId: "t1", source: "EVENT_STORE", eventType: "example.created", requestedBy: "ops" }
 */
public record ReplayRequest(
        String tenantId,
        String eventType,
        Instant fromTimestamp,
        Instant toTimestamp,
        List<UUID> specificIds,
        String requestedBy,
        ReplaySource source
) {
    public enum ReplaySource {
        /** Re-publish PUBLISHED rows from outbox_records — default. */
        OUTBOX,
        /** Re-publish from event_store (permanent log) — use for read-model rebuild. */
        EVENT_STORE
    }

    public ReplayRequest {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId is required");
        }
        if (requestedBy == null || requestedBy.isBlank()) {
            throw new IllegalArgumentException("requestedBy is required");
        }
        if (source == null) {
            source = ReplaySource.OUTBOX;
        }
    }
}
