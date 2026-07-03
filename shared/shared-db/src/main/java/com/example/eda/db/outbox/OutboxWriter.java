package com.example.eda.db.outbox;

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
 */
@Component
public class OutboxWriter {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxRecord write(String eventType, Object payload, String correlationId) {
        String tenantId = TenantContextHolder.get().tenantId();
        String payloadJson = serialize(payload);

        OutboxRecord record = OutboxRecord.builder()
                .eventId(UUID.randomUUID())
                .tenantId(tenantId)
                .eventType(eventType)
                .payload(payloadJson)
                .correlationId(correlationId)
                .build();

        return outboxRepository.save(record);
    }

    private String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new OutboxWriteException("Failed to serialize outbox payload", e);
        }
    }
}
