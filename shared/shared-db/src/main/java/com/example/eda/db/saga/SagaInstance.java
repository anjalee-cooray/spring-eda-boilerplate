package com.example.eda.db.saga;

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

/**
 * Persisted state of one running saga instance.
 *
 * Storing state in the DB rather than in-memory means the saga survives service
 * restarts and crashes. On recovery the orchestrator reloads the instance by
 * correlationId when the next response event arrives.
 *
 * context (JSONB) holds saga-specific fields serialised to JSON. The orchestrator
 * deserialises it into a domain-specific record at each step.
 */
@Entity
@Table(name = "saga_instances")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaInstance {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "saga_type", nullable = false)
    private String sagaType;

    /** Unique per saga run; echoed on every event emitted and consumed by this saga. */
    @Column(name = "correlation_id", nullable = false, unique = true)
    private String correlationId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private SagaStatus status = SagaStatus.STARTED;

    /** Name of the step the saga is currently waiting to complete. */
    @Column(name = "current_step", nullable = false)
    private String currentStep;

    /** Saga-specific data serialised as JSON — available at every step. */
    @Column(name = "context", nullable = false, columnDefinition = "jsonb")
    @Builder.Default
    private String context = "{}";

    /** Index into the compensation steps list — counts up as compensations run. */
    @Column(name = "compensation_step_index", nullable = false)
    @Builder.Default
    private int compensationStepIndex = 0;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    @Column(name = "completed_at")
    private Instant completedAt;

    // ── lifecycle transitions ──────────────────────────────────────────────

    public void advanceTo(String nextStep) {
        this.status = SagaStatus.RUNNING;
        this.currentStep = nextStep;
        this.updatedAt = Instant.now();
    }

    public void complete() {
        this.status = SagaStatus.COMPLETED;
        this.completedAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public void startCompensation(String reason) {
        this.status = SagaStatus.COMPENSATING;
        this.failureReason = reason;
        this.compensationStepIndex = 0;
        this.updatedAt = Instant.now();
    }

    public void advanceCompensation() {
        this.compensationStepIndex++;
        this.updatedAt = Instant.now();
    }

    public void markCompensated() {
        this.status = SagaStatus.COMPENSATED;
        this.completedAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public void markFailed(String reason) {
        this.status = SagaStatus.FAILED;
        this.failureReason = reason;
        this.completedAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public void updateContext(String contextJson) {
        this.context = contextJson;
        this.updatedAt = Instant.now();
    }

    public boolean isTerminal() {
        return status == SagaStatus.COMPLETED
                || status == SagaStatus.COMPENSATED
                || status == SagaStatus.FAILED;
    }
}
