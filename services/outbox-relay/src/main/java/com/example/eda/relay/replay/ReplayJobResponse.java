package com.example.eda.relay.replay;

import java.time.Instant;
import java.util.UUID;

public record ReplayJobResponse(
        UUID id,
        String tenantId,
        String eventType,
        Instant fromTimestamp,
        Instant toTimestamp,
        String specificIds,
        String status,
        Integer totalEvents,
        int replayedCount,
        String errorMessage,
        String requestedBy,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt
) {
    public static ReplayJobResponse from(ReplayJob job) {
        return new ReplayJobResponse(
                job.getId(),
                job.getTenantId(),
                job.getEventType(),
                job.getFromTimestamp(),
                job.getToTimestamp(),
                job.getSpecificIds(),
                job.getStatus().name(),
                job.getTotalEvents(),
                job.getReplayedCount(),
                job.getErrorMessage(),
                job.getRequestedBy(),
                job.getCreatedAt(),
                job.getStartedAt(),
                job.getCompletedAt()
        );
    }
}
