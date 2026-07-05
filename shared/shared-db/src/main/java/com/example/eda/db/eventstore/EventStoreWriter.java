package com.example.eda.db.eventstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes an immutable record to the event_store table.
 *
 * Always called from within OutboxWriter.write() in the same transaction, so the
 * event store record and the outbox record are written atomically. An event either
 * appears in both tables or neither — there is no state where the outbox has a record
 * that the event store does not.
 *
 * The event store is append-only: EventStoreWriter never updates or deletes rows.
 * To support event sourcing queries, pass aggregateId and aggregateType when the
 * event belongs to a specific domain entity.
 *
 * Usage — basic (called automatically from OutboxWriter):
 *   outboxWriter.write("example.created", payload, correlationId);
 *   // EventStoreWriter is called internally — no additional code needed
 *
 * Usage — with aggregate context (call directly when you have entity identity):
 *   eventStoreWriter.write(eventId, "order.confirmed", tenantId,
 *       "order-uuid", "Order", "1", payload, correlationId, null);
 */
@Component
public class EventStoreWriter {

    private final EventStoreRepository eventStoreRepository;
    private final ObjectMapper objectMapper;

    public EventStoreWriter(EventStoreRepository eventStoreRepository, ObjectMapper objectMapper) {
        this.eventStoreRepository = eventStoreRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Minimal write — used by OutboxWriter to mirror every outbox event automatically.
     * aggregateId, aggregateType, and causationId are null (set them via the full variant
     * when you have aggregate context).
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public EventStoreRecord write(
            UUID eventId, String eventType, String tenantId,
            String schemaVersion, Object payload, String correlationId) {
        return write(eventId, eventType, tenantId, null, null,
                schemaVersion, payload, correlationId, null);
    }

    /**
     * Full write — use this when the event belongs to a specific domain entity.
     * aggregateId and aggregateType enable the event-sourcing query:
     *   findByAggregateIdOrderByOccurredAtAsc(aggregateId)
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public EventStoreRecord write(
            UUID eventId, String eventType, String tenantId,
            String aggregateId, String aggregateType,
            String schemaVersion, Object payload,
            String correlationId, String causationId) {

        EventStoreRecord record = EventStoreRecord.builder()
                .eventId(eventId)
                .eventType(eventType)
                .tenantId(tenantId)
                .aggregateId(aggregateId)
                .aggregateType(aggregateType)
                .schemaVersion(schemaVersion)
                .payload(serialize(payload))
                .correlationId(correlationId)
                .causationId(causationId)
                .build();

        return eventStoreRepository.save(record);
    }

    private String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event store payload", e);
        }
    }
}
