package com.example.eda.db.inbox;

import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Idempotency guard for event consumers.
 * Call isDuplicate before processing — if true, skip the event entirely.
 * Call markProcessed after successful processing within the same transaction.
 */
@Component
public class InboxDeduplicator {

    private final InboxRepository inboxRepository;

    public InboxDeduplicator(InboxRepository inboxRepository) {
        this.inboxRepository = inboxRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean isDuplicate(UUID eventId) {
        return inboxRepository.existsByEventId(eventId);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void markProcessed(UUID eventId, String eventType, String tenantId) {
        InboxRecord record = InboxRecord.builder()
                .eventId(eventId)
                .eventType(eventType)
                .tenantId(tenantId)
                .build();
        inboxRepository.save(record);
    }
}
