package com.example.eda.relay;

import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Recovers outbox records orphaned by a relay crash.
 *
 * When the relay crashes between Phase 1 (claim) and Phase 3 (complete), the
 * affected records are left as IN_FLIGHT with a locked_until timestamp in the past.
 * This task runs periodically, finds those expired records, and either:
 *
 *   - Resets them to PENDING so the next relay poll picks them up (attempts remaining)
 *   - Marks them FAILED so an operator is alerted (attempts exhausted)
 *
 * The reclaimer does not re-publish events directly. It only resets the DB state.
 * The normal poll loop handles the re-publish on the next cycle. This separation
 * of concerns keeps both tasks simple and independently testable.
 *
 * Lock-timeout sizing guidance:
 *   Set lock-timeout-seconds to comfortably exceed the maximum time a single batch
 *   publish can take. For 50 records at ~10ms per broker call that is ~500ms, so the
 *   default of 30s provides a large safety margin. The reclaimer interval should be
 *   longer than the lock timeout to avoid reclaiming records still being processed
 *   by a slow (but alive) relay instance.
 */
@Component
public class OutboxReclaimTask {

    private static final Logger log = LoggerFactory.getLogger(OutboxReclaimTask.class);

    private final OutboxRepository outboxRepository;

    @Value("${app.relay.max-attempts:5}")
    private int maxAttempts;

    public OutboxReclaimTask(OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    @Scheduled(fixedDelayString = "${app.relay.reclaim-interval-ms:60000}")
    @Transactional
    public void reclaim() {
        List<OutboxRecord> expired = outboxRepository.findExpiredInFlight(Instant.now());

        if (expired.isEmpty()) {
            return;
        }

        log.warn("Found {} expired IN_FLIGHT outbox records — relay may have crashed", expired.size());

        List<UUID> toRetry = new ArrayList<>();
        List<UUID> toFail  = new ArrayList<>();

        for (OutboxRecord record : expired) {
            if (record.getAttemptCount() >= maxAttempts) {
                toFail.add(record.getId());
                log.error("Outbox record exhausted retries after crash recovery, marking FAILED. " +
                                "id={} eventType={} tenantId={} attempts={}",
                        record.getId(), record.getEventType(), record.getTenantId(), record.getAttemptCount());
            } else {
                toRetry.add(record.getId());
                log.warn("Reclaiming expired IN_FLIGHT outbox record for retry. " +
                                "id={} eventType={} tenantId={} attempt={}/{}",
                        record.getId(), record.getEventType(), record.getTenantId(),
                        record.getAttemptCount(), maxAttempts);
            }
        }

        if (!toRetry.isEmpty()) {
            for (UUID id : toRetry) {
                outboxRepository.findById(id).ifPresent(r -> {
                    r.resetToPending("Reclaimed after lock expiry (relay may have crashed)");
                    outboxRepository.save(r);
                });
            }
        }

        if (!toFail.isEmpty()) {
            for (UUID id : toFail) {
                outboxRepository.findById(id).ifPresent(r -> {
                    r.markFailed("Exhausted " + r.getAttemptCount() + " attempts including crash recovery");
                    outboxRepository.save(r);
                });
            }
        }
    }
}
