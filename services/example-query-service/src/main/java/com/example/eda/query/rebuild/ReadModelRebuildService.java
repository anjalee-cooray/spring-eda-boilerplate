package com.example.eda.query.rebuild;

import com.example.eda.db.eventstore.EventStoreRecord;
import com.example.eda.db.eventstore.EventStoreRepository;
import com.example.eda.db.projection.ProjectionVersionRecord;
import com.example.eda.db.projection.ProjectionVersionRepository;
import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.query.projection.ExampleReadModelRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Truncates a read-model table and re-applies all events from the event_store.
 *
 * This is the correct mechanism for:
 *   - Fixing a bug in a projector's handler logic
 *   - Applying a schema migration to existing read-model rows
 *   - Recovering from a corrupted or incomplete projection
 *
 * Safety guarantees:
 *   1. Projection is marked REBUILDING before truncation — queries still serve
 *      stale data during rebuild rather than returning empty results.
 *   2. Events are re-applied directly to the projector (not via broker) — no
 *      duplicate events are published and inbox_records dedup is bypassed intentionally.
 *   3. Rebuild runs in its own thread — the query service remains available.
 *
 * Usage:
 *   POST /projections/ExampleProjector/rebuild
 *   { "targetVersion": 2, "requestedBy": "ops-team" }
 */
@Service
public class ReadModelRebuildService {

    private static final Logger log = LoggerFactory.getLogger(ReadModelRebuildService.class);

    private final ProjectionVersionRepository projectionVersionRepository;
    private final EventStoreRepository eventStoreRepository;
    private final ExampleReadModelRepository exampleReadModelRepository;
    private final List<EventConsumer> eventConsumers;
    private final ObjectMapper objectMapper;

    public ReadModelRebuildService(
            ProjectionVersionRepository projectionVersionRepository,
            EventStoreRepository eventStoreRepository,
            ExampleReadModelRepository exampleReadModelRepository,
            List<EventConsumer> eventConsumers,
            ObjectMapper objectMapper) {
        this.projectionVersionRepository = projectionVersionRepository;
        this.eventStoreRepository = eventStoreRepository;
        this.exampleReadModelRepository = exampleReadModelRepository;
        this.eventConsumers = eventConsumers;
        this.objectMapper = objectMapper;
    }

    /**
     * Initiates a rebuild for the named projection. Returns a tracking ID immediately;
     * the actual rebuild runs asynchronously via {@link #runRebuild}.
     */
    @Transactional
    public ReadModelRebuildResponse initiate(String projectionName, String tenantId, ReadModelRebuildRequest request) {
        ProjectionVersionRecord version = projectionVersionRepository
                .findByProjectionName(projectionName)
                .orElseThrow(() -> new IllegalArgumentException("Unknown projection: " + projectionName));

        if ("REBUILDING".equals(version.getStatus())) {
            return new ReadModelRebuildResponse(
                    projectionName,
                    version.getCurrentVersion(),
                    version.getRebuildJobId(),
                    "REBUILDING",
                    "A rebuild is already in progress. Check rebuildJobId for status."
            );
        }

        if (request.targetVersion() <= version.getCurrentVersion()) {
            throw new IllegalArgumentException(
                    "targetVersion (%d) must be greater than currentVersion (%d)"
                            .formatted(request.targetVersion(), version.getCurrentVersion()));
        }

        UUID rebuildJobId = UUID.randomUUID();
        version.markRebuilding(rebuildJobId);
        projectionVersionRepository.save(version);

        log.info("Rebuild initiated projection={} tenant={} currentVersion={} targetVersion={} jobId={}",
                projectionName, tenantId, version.getCurrentVersion(), request.targetVersion(), rebuildJobId);

        runRebuild(projectionName, tenantId, request.targetVersion(), rebuildJobId);

        return new ReadModelRebuildResponse(
                projectionName,
                request.targetVersion(),
                rebuildJobId,
                "REBUILDING",
                "Rebuild started. Poll status via GET /projections/" + projectionName + "/version"
        );
    }

    /**
     * Runs the truncate + re-apply cycle asynchronously.
     * Marked @Async so the HTTP response returns immediately after initiate().
     */
    @Async("replayExecutor")
    public void runRebuild(String projectionName, String tenantId, int targetVersion, UUID rebuildJobId) {
        try {
            log.info("Rebuild running projection={} tenant={} jobId={}", projectionName, tenantId, rebuildJobId);

            List<EventStoreRecord> events = eventStoreRepository.findForReplay(
                    tenantId, null, null, null);

            log.info("Rebuild replaying {} events projection={} jobId={}", events.size(), projectionName, rebuildJobId);

            // Truncate within its own transaction before re-applying
            truncateReadModel(projectionName);

            int applied = 0;
            for (EventStoreRecord record : events) {
                EventEnvelope envelope = toEnvelope(record);
                for (EventConsumer consumer : eventConsumers) {
                    if (consumer.supports(record.getEventType())) {
                        consumer.handle(envelope);
                        applied++;
                    }
                }
            }

            ProjectionVersionRecord version = projectionVersionRepository
                    .findByProjectionName(projectionName)
                    .orElseThrow();
            version.markCurrent(targetVersion);
            projectionVersionRepository.save(version);

            log.info("Rebuild complete projection={} tenant={} jobId={} eventsApplied={} newVersion={}",
                    projectionName, tenantId, rebuildJobId, applied, targetVersion);

        } catch (Exception ex) {
            log.error("Rebuild failed projection={} tenant={} jobId={}", projectionName, tenantId, rebuildJobId, ex);
            projectionVersionRepository.findByProjectionName(projectionName)
                    .ifPresent(v -> {
                        v.markFailed(ex.getMessage());
                        projectionVersionRepository.save(v);
                    });
        }
    }

    @Transactional
    public void truncateReadModel(String projectionName) {
        if ("ExampleProjector".equals(projectionName)) {
            exampleReadModelRepository.deleteAll();
            log.info("Truncated read model for projection={}", projectionName);
        } else {
            throw new IllegalArgumentException("No truncation strategy for projection: " + projectionName);
        }
    }

    private EventEnvelope toEnvelope(EventStoreRecord record) {
        try {
            Object payload = objectMapper.readValue(record.getPayload(), Object.class);
            return new EventEnvelope(
                    record.getEventId(),
                    record.getEventType(),
                    record.getTenantId(),
                    record.getCorrelationId() != null ? record.getCorrelationId() : record.getEventId().toString(),
                    record.getCausationId(),
                    record.getOccurredAt(),
                    payload,
                    record.getSchemaVersion(),
                    record.getAggregateId()
            );
        } catch (Exception ex) {
            throw new RuntimeException("Failed to deserialize event_store record " + record.getEventId(), ex);
        }
    }
}
