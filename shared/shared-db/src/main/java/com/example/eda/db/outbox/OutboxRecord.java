package com.example.eda.db.outbox;

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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "outbox_records")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "schema_version", nullable = false)
    @Builder.Default
    private String schemaVersion = "1";

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "locked_until")
    private Instant lockedUntil;

    @Column(name = "attempt_count", nullable = false)
    @Builder.Default
    private int attemptCount = 0;

    @Column(name = "last_error")
    private String lastError;

    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = Instant.now();
        this.lockedUntil = null;
        this.lastError = null;
    }

    public void markInFlight(Instant lockedUntil) {
        this.status = OutboxStatus.IN_FLIGHT;
        this.lockedUntil = lockedUntil;
        this.attemptCount++;
    }

    // Called by OutboxReclaimTask or OutboxRelayPoller when publish fails and
    // attempts remain. The record will be picked up again on the next poll.
    public void resetToPending(String error) {
        this.status = OutboxStatus.PENDING;
        this.lockedUntil = null;
        this.lastError = error;
    }

    public void markFailed(String error) {
        this.status = OutboxStatus.FAILED;
        this.lockedUntil = null;
        this.lastError = error;
    }

    public enum OutboxStatus {
        PENDING, IN_FLIGHT, PUBLISHED, FAILED
    }
}
