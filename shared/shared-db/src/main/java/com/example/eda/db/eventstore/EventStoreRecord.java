package com.example.eda.db.eventstore;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Immutable record in the permanent event log.
 *
 * Written once by EventStoreWriter in the same transaction as OutboxRecord.
 * Never updated, never deleted. Use this table for:
 *
 *   Audit queries  — "what happened to booking X?"
 *   Event replay   — re-publish events without depending on broker retention
 *   Event sourcing — reconstruct aggregate state by replaying its event stream
 *
 * aggregate_id / aggregate_type are optional. Set them when the event belongs
 * to a specific domain entity (e.g. appointment_id, order_id) so you can query
 * "all events for this aggregate" efficiently.
 */
@Entity
@Table(name = "event_store")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventStoreRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    /** Optional: the domain entity this event belongs to (e.g. "booking-uuid"). */
    @Column(name = "aggregate_id")
    private String aggregateId;

    /** Optional: the domain entity type (e.g. "Booking", "Order"). */
    @Column(name = "aggregate_type")
    private String aggregateType;

    @Column(name = "schema_version", nullable = false)
    @Builder.Default
    private String schemaVersion = "1";

    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "causation_id")
    private String causationId;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant occurredAt = Instant.now();
}
