package com.example.eda.db.projection;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
import lombok.Setter;

/**
 * Tracks the version and rebuild status of each CQRS read-side projection.
 *
 * When a projection's handler logic changes, increment currentVersion and trigger
 * a rebuild via POST /projections/{name}/rebuild. The service sets status=REBUILDING,
 * launches an EVENT_STORE replay job, then sets CURRENT on completion.
 */
@Entity
@Table(name = "projection_versions")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProjectionVersionRecord {

    public enum ProjectionStatus {
        CURRENT, REBUILDING, FAILED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "projection_name", nullable = false, unique = true)
    private String projectionName;

    @Column(name = "current_version", nullable = false)
    @Builder.Default
    private int currentVersion = 1;

    @Column(name = "status", nullable = false)
    @Builder.Default
    private String status = ProjectionStatus.CURRENT.name();

    @Column(name = "last_rebuilt_at")
    private Instant lastRebuiltAt;

    @Column(name = "rebuild_job_id")
    private UUID rebuildJobId;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    public void markRebuilding(UUID replayJobId) {
        this.status = ProjectionStatus.REBUILDING.name();
        this.rebuildJobId = replayJobId;
        this.errorMessage = null;
        this.updatedAt = Instant.now();
    }

    public void markCurrent(int newVersion) {
        this.status = ProjectionStatus.CURRENT.name();
        this.currentVersion = newVersion;
        this.lastRebuiltAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public void markFailed(String error) {
        this.status = ProjectionStatus.FAILED.name();
        this.errorMessage = error;
        this.updatedAt = Instant.now();
    }
}
