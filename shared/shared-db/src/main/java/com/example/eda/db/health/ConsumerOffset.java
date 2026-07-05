package com.example.eda.db.health;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Tracks the last successfully processed event for one (consumer, event_type, tenant) tuple.
 * Updated after every successful EventConsumer.handle() call.
 */
@Entity
@Table(name = "consumer_offsets")
@IdClass(ConsumerOffset.ConsumerOffsetId.class)
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerOffset {

    @Id
    @Column(name = "consumer_name", nullable = false)
    private String consumerName;

    @Id
    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Id
    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "last_event_id")
    private UUID lastEventId;

    @Column(name = "last_event_at")
    private Instant lastEventAt;

    @Column(name = "events_processed", nullable = false)
    @Builder.Default
    private long eventsProcessed = 0;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "last_error_at")
    private Instant lastErrorAt;

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    public void recordSuccess(UUID eventId, Instant eventAt) {
        this.lastEventId = eventId;
        this.lastEventAt = eventAt;
        this.eventsProcessed++;
        this.updatedAt = Instant.now();
    }

    public void recordError(String error) {
        this.lastError = error;
        this.lastErrorAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public static class ConsumerOffsetId implements Serializable {
        private String consumerName;
        private String eventType;
        private String tenantId;

        public ConsumerOffsetId() {}

        public ConsumerOffsetId(String consumerName, String eventType, String tenantId) {
            this.consumerName = consumerName;
            this.eventType = eventType;
            this.tenantId = tenantId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ConsumerOffsetId that)) return false;
            return java.util.Objects.equals(consumerName, that.consumerName)
                    && java.util.Objects.equals(eventType, that.eventType)
                    && java.util.Objects.equals(tenantId, that.tenantId);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(consumerName, eventType, tenantId);
        }
    }
}
