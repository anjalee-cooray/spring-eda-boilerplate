package com.example.eda.relay;

import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Atomically claims a batch of PENDING outbox records for exclusive processing.
 *
 * The claim is a two-step operation inside a single REQUIRES_NEW transaction:
 *
 *   Step 1 — SELECT FOR UPDATE SKIP LOCKED
 *     Locks the chosen rows. SKIP LOCKED skips any rows already locked by another
 *     relay instance, so two relay pods running simultaneously always work on
 *     disjoint batches — no double-publish from concurrent relays.
 *
 *   Step 2 — UPDATE status → IN_FLIGHT
 *     Within the same transaction, marks the locked rows as IN_FLIGHT and sets
 *     locked_until = now() + lockTimeout. The lock timeout acts as a dead-man's
 *     switch: if the relay crashes after claiming but before completing the publish,
 *     OutboxReclaimTask resets expired IN_FLIGHT records back to PENDING.
 *
 *   Step 3 — load full entities
 *     After the bulk UPDATE clears the Hibernate first-level cache (clearAutomatically),
 *     findAllById() returns the fresh IN_FLIGHT state ready for the poller to process.
 *
 * Using REQUIRES_NEW ensures the claim commits (and locks are released) before the
 * caller starts the broker publish. The broker call is intentionally outside any
 * transaction — it is not a DB operation and must not hold a DB connection open.
 */
@Service
public class OutboxClaimService {

    private static final Logger log = LoggerFactory.getLogger(OutboxClaimService.class);

    private final OutboxRepository outboxRepository;

    public OutboxClaimService(OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<OutboxRecord> claimBatch(int batchSize, Duration lockTimeout) {
        List<UUID> ids = outboxRepository.findPendingIdsForUpdate(batchSize).stream()
                .map(raw -> raw instanceof UUID u ? u : UUID.fromString(raw.toString()))
                .toList();

        if (ids.isEmpty()) {
            return List.of();
        }

        Instant lockedUntil = Instant.now().plus(lockTimeout);
        outboxRepository.markInFlight(ids, lockedUntil);

        List<OutboxRecord> claimed = outboxRepository.findAllById(ids);
        log.debug("Claimed {} outbox records, lockedUntil={}", claimed.size(), lockedUntil);
        return claimed;
    }
}
