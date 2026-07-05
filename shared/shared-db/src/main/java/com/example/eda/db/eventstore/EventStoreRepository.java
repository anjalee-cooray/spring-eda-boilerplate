package com.example.eda.db.eventstore;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface EventStoreRepository extends JpaRepository<EventStoreRecord, UUID> {

    /** All events for a tenant within a time range — primary audit query. */
    @Query("""
            SELECT e FROM EventStoreRecord e
            WHERE e.tenantId   = :tenantId
            AND   (:from       IS NULL OR e.occurredAt >= :from)
            AND   (:to         IS NULL OR e.occurredAt <= :to)
            AND   (:eventType  IS NULL OR e.eventType  = :eventType)
            ORDER BY e.occurredAt ASC
            """)
    List<EventStoreRecord> findByTenant(
            @Param("tenantId")  String  tenantId,
            @Param("eventType") String  eventType,
            @Param("from")      Instant from,
            @Param("to")        Instant to,
            Pageable            pageable
    );

    /** All events for a specific aggregate in chronological order — event sourcing. */
    List<EventStoreRecord> findByAggregateIdOrderByOccurredAtAsc(String aggregateId);

    /** Audit: find a single event by its stable business ID. */
    java.util.Optional<EventStoreRecord> findByEventId(UUID eventId);
}
