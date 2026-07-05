package com.example.eda.relay.replay;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "replay_jobs")
public class ReplayJob {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "event_type")
    private String eventType;

    @Column(name = "from_timestamp")
    private Instant fromTimestamp;

    @Column(name = "to_timestamp")
    private Instant toTimestamp;

    // Comma-separated outbox record UUIDs — null means scope is query-based
    @Column(name = "specific_ids", columnDefinition = "TEXT")
    private String specificIds;

    @Column(name = "replay_source", nullable = false)
    private String replaySource = "OUTBOX";

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ReplayStatus status = ReplayStatus.PENDING;

    @Column(name = "total_events")
    private Integer totalEvents;

    @Column(name = "replayed_count", nullable = false)
    private int replayedCount = 0;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "requested_by", nullable = false)
    private String requestedBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    public enum ReplayStatus {
        PENDING, RUNNING, COMPLETED, FAILED
    }

    protected ReplayJob() {}

    public static ReplayJob create(ReplayRequest request) {
        ReplayJob job = new ReplayJob();
        job.tenantId      = request.tenantId();
        job.eventType     = request.eventType();
        job.fromTimestamp = request.fromTimestamp();
        job.toTimestamp   = request.toTimestamp();
        job.specificIds   = request.specificIds() != null
                ? String.join(",", request.specificIds().stream().map(UUID::toString).toList())
                : null;
        job.requestedBy   = request.requestedBy();
        job.replaySource  = request.source() != null ? request.source().name() : "OUTBOX";
        return job;
    }

    public void markRunning(int totalEvents) {
        this.status      = ReplayStatus.RUNNING;
        this.totalEvents = totalEvents;
        this.startedAt   = Instant.now();
    }

    public void incrementReplayed() {
        this.replayedCount++;
    }

    public void markCompleted() {
        this.status      = ReplayStatus.COMPLETED;
        this.completedAt = Instant.now();
    }

    public void markFailed(String errorMessage) {
        this.status       = ReplayStatus.FAILED;
        this.errorMessage = errorMessage;
        this.completedAt  = Instant.now();
    }

    public UUID getId()               { return id; }
    public String getTenantId()       { return tenantId; }
    public String getEventType()      { return eventType; }
    public Instant getFromTimestamp() { return fromTimestamp; }
    public Instant getToTimestamp()   { return toTimestamp; }
    public String getSpecificIds()    { return specificIds; }
    public ReplayStatus getStatus()   { return status; }
    public Integer getTotalEvents()   { return totalEvents; }
    public int getReplayedCount()     { return replayedCount; }
    public String getErrorMessage()   { return errorMessage; }
    public String getRequestedBy()    { return requestedBy; }
    public String getReplaySource()   { return replaySource; }
    public Instant getCreatedAt()     { return createdAt; }
    public Instant getStartedAt()     { return startedAt; }
    public Instant getCompletedAt()   { return completedAt; }
}
