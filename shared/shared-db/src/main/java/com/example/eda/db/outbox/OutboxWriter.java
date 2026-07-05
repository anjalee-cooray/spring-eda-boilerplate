package com.example.eda.db.outbox;

import com.example.eda.db.eventstore.EventStoreWriter;
import com.example.eda.security.TenantContextHolder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes an outbox_records row within the caller's active transaction.
 * Never call outside a transaction — the outbox write must be atomic with the domain write.
 *
 * Every outbox write also mirrors an immutable record to event_store in the same
 * transaction. This guarantees that the permanent event log and the relay queue
 * are always in sync — an event either appears in both tables or neither.
 */
@Component
public class OutboxWriter {

    private final OutboxRepository outboxRepository;
    private final EventStoreWriter eventStoreWriter;
    private final ObjectMapper objectMapper;

    public OutboxWriter(
            OutboxRepository outboxRepository,
            EventStoreWriter eventStoreWriter,
            ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.eventStoreWriter = eventStoreWriter;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxRecord write(String eventType, Object payload, String correlationId) {
        return write(eventType, "1", payload, correlationId);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxRecord write(String eventType, String schemaVersion, Object payload, String correlationId) {
        String tenantId = TenantContextHolder.get().tenantId();
        UUID eventId = UUID.randomUUID();
        String payloadJson = serialize(payload);

        OutboxRecord record = OutboxRecord.builder()
                .eventId(eventId)
                .tenantId(tenantId)
                .eventType(eventType)
                .schemaVersion(schemaVersion)
                .payload(payloadJson)
                .correlationId(correlationId)
                .build();

        outboxRepository.save(record);

        // Mirror to the permanent event store in the same transaction.
        // The outbox record is transient (status changes, eventually cleaned up).
        // The event store record is immutable — it is the audit log and replay source.
        eventStoreWriter.write(eventId, eventType, tenantId, schemaVersion, payload, correlationId);

        return record;
    }

    private String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new OutboxWriteException("Failed to serialize outbox payload", e);
        }
    }
}
