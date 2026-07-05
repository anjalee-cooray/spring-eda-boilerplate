package com.example.eda.relay.replay;

import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxRepository;
import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.publisher.EventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Executes event replay jobs asynchronously.
 *
 * Replay is safe by design:
 *   Events are re-published with their original event_id. The idempotent consumer
 *   pattern (inbox_records deduplication) ensures consumers that already processed
 *   an event skip it silently. New or reset consumers process it normally.
 *
 * Broker-agnostic:
 *   Replay publishes via EventPublisher — the same code path works for both Kafka
 *   and SNS/SQS. Switch EVENTS_BROKER in config; replay behaviour is unchanged.
 *
 * Concurrency:
 *   Each job runs in a dedicated thread from the "replay" executor (configured via
 *   @EnableAsync). The outbox poller continues running in parallel — normal event
 *   flow is not interrupted during replay.
 *
 * Progress tracking:
 *   replayedCount is updated and persisted after each event. If the service
 *   crashes mid-replay, the job remains RUNNING — restart and create a new job
 *   for the remaining date range. Consumers skip already-processed events via inbox dedup.
 */
@Service
public class ReplayJobService {

    private static final Logger log = LoggerFactory.getLogger(ReplayJobService.class);

    private final ReplayJobRepository replayJobRepository;
    private final OutboxRepository outboxRepository;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final Counter replayedCounter;

    public ReplayJobService(
            ReplayJobRepository replayJobRepository,
            OutboxRepository outboxRepository,
            EventPublisher eventPublisher,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.replayJobRepository = replayJobRepository;
        this.outboxRepository    = outboxRepository;
        this.eventPublisher      = eventPublisher;
        this.objectMapper        = objectMapper;
        this.replayedCounter     = Counter.builder("replay.events.published")
                .description("Total events re-published during replay jobs")
                .register(meterRegistry);
    }

    /**
     * Creates a replay job record and returns immediately.
     * The actual replay runs asynchronously — poll GET /replay/jobs/{id} for progress.
     */
    @Transactional
    public ReplayJob create(ReplayRequest request) {
        ReplayJob job = ReplayJob.create(request);
        replayJobRepository.save(job);
        log.info("Replay job created id={} tenantId={} eventType={} from={} to={} specificIds={} requestedBy={}",
                job.getId(), job.getTenantId(), job.getEventType(),
                job.getFromTimestamp(), job.getToTimestamp(),
                job.getSpecificIds(), job.getRequestedBy());
        return job;
    }

    /**
     * Executes the replay job in a background thread.
     * Called immediately after create() from ReplayController.
     */
    @Async("replayExecutor")
    public void execute(UUID jobId) {
        ReplayJob job = replayJobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Replay job not found: " + jobId));

        List<OutboxRecord> records = resolveRecords(job);

        job.markRunning(records.size());
        replayJobRepository.save(job);

        log.info("Replay job started id={} totalEvents={}", job.getId(), records.size());

        try {
            for (OutboxRecord record : records) {
                publishRecord(record);
                job.incrementReplayed();
                replayedCounter.increment();

                // Persist progress every 100 events so progress is visible without flooding DB
                if (job.getReplayedCount() % 100 == 0) {
                    replayJobRepository.save(job);
                    log.info("Replay progress id={} replayed={}/{}", job.getId(),
                            job.getReplayedCount(), job.getTotalEvents());
                }
            }

            job.markCompleted();
            replayJobRepository.save(job);
            log.info("Replay job completed id={} totalReplayed={}", job.getId(), job.getReplayedCount());

        } catch (Exception ex) {
            job.markFailed(ex.getMessage());
            replayJobRepository.save(job);
            log.error("Replay job failed id={} replayedBeforeFailure={}", job.getId(), job.getReplayedCount(), ex);
        }
    }

    private List<OutboxRecord> resolveRecords(ReplayJob job) {
        if (job.getSpecificIds() != null && !job.getSpecificIds().isBlank()) {
            List<UUID> ids = Arrays.stream(job.getSpecificIds().split(","))
                    .map(String::trim)
                    .map(UUID::fromString)
                    .toList();
            return outboxRepository.findAllByIdIn(ids);
        }

        return outboxRepository.findPublishedForReplay(
                job.getTenantId(),
                job.getEventType(),
                job.getFromTimestamp(),
                job.getToTimestamp()
        );
    }

    private void publishRecord(OutboxRecord record) {
        try {
            Object payload = objectMapper.readValue(record.getPayload(), Object.class);
            EventEnvelope envelope = EventEnvelope.builder()
                    .eventType(record.getEventType())
                    .tenantId(record.getTenantId())
                    .correlationId(record.getCorrelationId() != null
                            ? record.getCorrelationId()
                            : record.getEventId().toString())
                    .payload(payload)
                    .build();

            // Re-publish with the original event_id (inbox dedup makes replay safe)
            // and the original schema_version so consumers apply the correct upcaster chain.
            EventEnvelope withOriginalId = new EventEnvelope(
                    record.getEventId(),
                    envelope.eventType(),
                    envelope.tenantId(),
                    envelope.correlationId(),
                    envelope.causationId(),
                    envelope.occurredAt(),
                    envelope.payload(),
                    record.getSchemaVersion(),
                    record.getAggregateId()  // preserves original partition key on replay
            );

            eventPublisher.publish(withOriginalId);

            log.debug("Replayed event eventId={} eventType={} tenantId={}",
                    record.getEventId(), record.getEventType(), record.getTenantId());
        } catch (Exception ex) {
            log.error("Failed to replay outbox record id={} eventType={}", record.getId(), record.getEventType(), ex);
            throw new RuntimeException("Failed to publish outbox record " + record.getId(), ex);
        }
    }
}
