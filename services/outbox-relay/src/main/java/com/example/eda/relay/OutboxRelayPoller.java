package com.example.eda.relay;

import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxRepository;
import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.publisher.EventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Polls the outbox_records table and publishes PENDING events to the broker.
 *
 * At-least-once delivery guarantee — three-phase pattern:
 *
 *   Phase 1 — CLAIM  (REQUIRES_NEW transaction, commits before Phase 2)
 *     OutboxClaimService atomically acquires a batch via SELECT FOR UPDATE SKIP LOCKED
 *     and transitions the rows to IN_FLIGHT. The transaction commits before any broker
 *     call is made — no DB connection held open during network I/O.
 *
 *   Phase 2 — PUBLISH  (no transaction)
 *     EventPublisher sends the event to Kafka or SNS. The original event_id from the
 *     outbox record is preserved so consumer inbox deduplication works correctly even
 *     when the same event is published more than once.
 *
 *   Phase 3 — COMPLETE  (REQUIRES_NEW transaction per record)
 *     Success → PUBLISHED.
 *     Failure, attempts remaining → PENDING (retried on next poll).
 *     Failure, attempts exhausted → FAILED (requires operator action).
 *
 * Crash recovery:
 *   If the relay crashes between Phase 1 and Phase 3, records stay IN_FLIGHT with a
 *   locked_until timestamp. OutboxReclaimTask periodically finds expired IN_FLIGHT
 *   records and resets them to PENDING (or FAILED if attempts are exhausted).
 *
 * Duplicate safety:
 *   Consumers deduplicate by event_id via inbox_records. Because Phase 2 always uses
 *   the original event_id from the outbox row, at-least-once duplicates are silently
 *   discarded at the consumer — no business logic runs twice.
 *
 * Horizontal scaling:
 *   SKIP LOCKED ensures multiple relay pods work on disjoint batches. Running two
 *   relay instances doubles throughput without producing duplicates.
 */
@Component
public class OutboxRelayPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayPoller.class);

    private final OutboxClaimService claimService;
    private final OutboxRepository outboxRepository;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final OutboxRelayShutdownCoordinator shutdownCoordinator;
    private final Counter publishedCounter;
    private final Counter failedCounter;

    @Value("${app.relay.batch-size:50}")
    private int batchSize;

    @Value("${app.relay.max-attempts:5}")
    private int maxAttempts;

    @Value("${app.relay.lock-timeout-seconds:30}")
    private int lockTimeoutSeconds;

    public OutboxRelayPoller(
            OutboxClaimService claimService,
            OutboxRepository outboxRepository,
            EventPublisher eventPublisher,
            ObjectMapper objectMapper,
            OutboxRelayShutdownCoordinator shutdownCoordinator,
            MeterRegistry meterRegistry) {
        this.claimService = claimService;
        this.outboxRepository = outboxRepository;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
        this.shutdownCoordinator = shutdownCoordinator;

        this.publishedCounter = Counter.builder("outbox.relay.published")
                .description("Total events successfully published by the outbox relay")
                .register(meterRegistry);

        this.failedCounter = Counter.builder("outbox.relay.failed")
                .description("Total events that exhausted all retry attempts (FAILED status)")
                .register(meterRegistry);

        // Alert when records are permanently stuck in FAILED status
        Gauge.builder("outbox.records.failed", outboxRepository,
                r -> (double) r.countByStatus(OutboxRecord.OutboxStatus.FAILED))
                .description("Number of outbox records in FAILED status — requires operator attention")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${app.relay.poll-interval-ms:1000}")
    public void poll() {
        if (!shutdownCoordinator.isPollingEnabled()) {
            log.debug("Outbox polling skipped — shutdown in progress");
            return;
        }

        Duration lockTimeout = Duration.ofSeconds(lockTimeoutSeconds);

        // Phase 1: claim — commits before Phase 2 begins
        List<OutboxRecord> claimed = claimService.claimBatch(batchSize, lockTimeout);
        if (claimed.isEmpty()) {
            return;
        }

        log.debug("Relaying {} outbox records", claimed.size());

        for (OutboxRecord record : claimed) {
            // Phase 2: publish — no active transaction
            boolean published = publishRecord(record);

            // Phase 3: complete — short REQUIRES_NEW transaction
            completeRecord(record, published);
        }
    }

    private boolean publishRecord(OutboxRecord record) {
        try {
            eventPublisher.publish(buildEnvelope(record));
            return true;
        } catch (Exception ex) {
            log.error("Failed to publish outbox record id={} eventType={} attempt={}/{}",
                    record.getId(), record.getEventType(), record.getAttemptCount(), maxAttempts, ex);
            return false;
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeRecord(OutboxRecord record, boolean published) {
        // Reload inside the new transaction — the record object is detached
        OutboxRecord fresh = outboxRepository.findById(record.getId()).orElse(null);
        if (fresh == null) {
            log.warn("Outbox record disappeared before completion id={}", record.getId());
            return;
        }

        if (published) {
            fresh.markPublished();
            outboxRepository.save(fresh);
            log.debug("Published outbox record id={} eventType={}", fresh.getId(), fresh.getEventType());
            publishedCounter.increment();
        } else {
            String error = "Publish failed (attempt " + fresh.getAttemptCount() + "/" + maxAttempts + ")";
            if (fresh.getAttemptCount() >= maxAttempts) {
                fresh.markFailed(error);
                outboxRepository.save(fresh);
                log.error("Outbox record exhausted retries, marked FAILED — operator action required. id={} eventType={}",
                        fresh.getId(), fresh.getEventType());
                failedCounter.increment();
            } else {
                fresh.resetToPending(error);
                outboxRepository.save(fresh);
                log.warn("Outbox record publish failed, will retry. id={} eventType={} attempt={}/{}",
                        fresh.getId(), fresh.getEventType(), fresh.getAttemptCount(), maxAttempts);
            }
        }
    }

    private EventEnvelope buildEnvelope(OutboxRecord record) {
        Object payload = deserializePayload(record.getPayload());

        // Build with auto-generated eventId first (sets the other fields), then
        // override with the original event_id from the outbox record.
        // Using the original ID is critical: if this record is published more than once
        // (at-least-once delivery), the consumer's inbox_records deduplication catches
        // the duplicate by event_id. A freshly generated UUID here would bypass that
        // and cause the consumer to process the event twice.
        EventEnvelope base = EventEnvelope.builder()
                .eventType(record.getEventType())
                .tenantId(record.getTenantId())
                .correlationId(record.getCorrelationId() != null
                        ? record.getCorrelationId()
                        : record.getEventId().toString())
                .schemaVersion(record.getSchemaVersion())
                .payload(payload)
                .build();

        return new EventEnvelope(
                record.getEventId(),     // must be the original — not auto-generated
                base.eventType(),
                base.tenantId(),
                base.correlationId(),
                base.causationId(),
                base.occurredAt(),
                base.payload(),
                base.schemaVersion(),
                record.getAggregateId() // preserved for deterministic Kafka partition routing
        );
    }

    private Object deserializePayload(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson, Object.class);
        } catch (Exception e) {
            log.warn("Could not deserialize outbox payload as JSON, relaying as raw string");
            return payloadJson;
        }
    }
}
