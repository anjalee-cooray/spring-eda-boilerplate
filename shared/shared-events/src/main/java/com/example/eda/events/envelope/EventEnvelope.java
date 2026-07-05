package com.example.eda.events.envelope;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

/**
 * Standard wrapper for every domain event that travels over the message broker.
 *
 * aggregate_id is used as the Kafka partition key when present. All events for
 * the same aggregate_id (e.g. the same Order, Booking, or Account) are routed
 * to the same Kafka partition, guaranteeing FIFO delivery for that entity. The
 * key is prefixed with tenant_id to prevent cross-tenant partition collisions.
 *
 *   partitionKey = tenantId + ":" + aggregateId   (when aggregateId is set)
 *   partitionKey = tenantId                        (fallback for non-entity events)
 *
 * schema_version identifies which payload schema version was used at publish time.
 * Consumers apply an EventUpcaster chain to normalise older events to the latest
 * schema before dispatching to handlers.
 *
 * Backward compatibility: schema_version and aggregate_id are optional in JSON —
 * schema_version defaults to "1"; aggregate_id defaults to null (tenant-level routing).
 */
public record EventEnvelope(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("correlation_id") String correlationId,
        @JsonProperty("causation_id") String causationId,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("payload") Object payload,
        @JsonProperty("schema_version") String schemaVersion,
        @JsonProperty("aggregate_id") String aggregateId
) {

    public EventEnvelope {
        if (eventId == null) {
            throw new IllegalArgumentException("eventId must not be null");
        }
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (correlationId == null || correlationId.isBlank()) {
            throw new IllegalArgumentException("correlationId must not be blank");
        }
        if (occurredAt == null) {
            throw new IllegalArgumentException("occurredAt must not be null");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        // Default to "1" when deserialising events that pre-date schema versioning
        schemaVersion = schemaVersion != null ? schemaVersion : "1";
        // aggregateId may be null — not all events belong to a specific entity
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private UUID eventId = UUID.randomUUID();
        private String eventType;
        private String tenantId;
        private String correlationId;
        private String causationId;
        private Instant occurredAt = Instant.now();
        private Object payload;
        private String schemaVersion = "1";
        private String aggregateId;

        public Builder eventType(String eventType) { this.eventType = eventType; return this; }
        public Builder tenantId(String tenantId) { this.tenantId = tenantId; return this; }
        public Builder correlationId(String correlationId) { this.correlationId = correlationId; return this; }
        public Builder causationId(String causationId) { this.causationId = causationId; return this; }
        public Builder occurredAt(Instant occurredAt) { this.occurredAt = occurredAt; return this; }
        public Builder payload(Object payload) { this.payload = payload; return this; }
        public Builder schemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; return this; }
        public Builder aggregateId(String aggregateId) { this.aggregateId = aggregateId; return this; }

        // Explicitly set the event ID — use only when preserving an existing event_id (e.g. outbox relay).
        public Builder eventId(UUID eventId) { this.eventId = eventId; return this; }

        public EventEnvelope build() {
            return new EventEnvelope(
                    eventId, eventType, tenantId, correlationId, causationId,
                    occurredAt, payload, schemaVersion, aggregateId);
        }
    }
}
