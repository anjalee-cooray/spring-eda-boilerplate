package com.example.eda.events.envelope;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

/**
 * Standard wrapper for every domain event that travels over the message broker.
 *
 * schema_version identifies which payload schema version was used at publish time.
 * Consumers apply an EventUpcaster chain to normalise older events to the latest
 * schema before dispatching to handlers.
 *
 * Backward compatibility: schema_version is optional in JSON — events published
 * before this field was introduced deserialise to "1" via the compact constructor.
 */
public record EventEnvelope(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("correlation_id") String correlationId,
        @JsonProperty("causation_id") String causationId,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("payload") Object payload,
        @JsonProperty("schema_version") String schemaVersion
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

        public Builder eventType(String eventType) { this.eventType = eventType; return this; }
        public Builder tenantId(String tenantId) { this.tenantId = tenantId; return this; }
        public Builder correlationId(String correlationId) { this.correlationId = correlationId; return this; }
        public Builder causationId(String causationId) { this.causationId = causationId; return this; }
        public Builder occurredAt(Instant occurredAt) { this.occurredAt = occurredAt; return this; }
        public Builder payload(Object payload) { this.payload = payload; return this; }
        public Builder schemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; return this; }

        public EventEnvelope build() {
            return new EventEnvelope(
                    eventId, eventType, tenantId, correlationId, causationId,
                    occurredAt, payload, schemaVersion);
        }
    }
}
