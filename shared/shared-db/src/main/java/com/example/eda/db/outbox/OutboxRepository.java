package com.example.eda.db.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxRecord, UUID> {

    @Query("SELECT o FROM OutboxRecord o WHERE o.status = 'PENDING' ORDER BY o.createdAt ASC LIMIT :limit")
    List<OutboxRecord> findPendingRecords(int limit);

    /**
     * Finds PUBLISHED records for replay, with optional filters.
     * All filters are optional — pass null to skip that filter.
     *
     * Used by ReplayJobService to determine which events to re-publish.
     * Results are ordered oldest-first so replayed events arrive in the original order.
     */
    @Query("""
            SELECT o FROM OutboxRecord o
            WHERE o.tenantId    = :tenantId
            AND   o.status      = 'PUBLISHED'
            AND   (:eventType   IS NULL OR o.eventType  = :eventType)
            AND   (:from        IS NULL OR o.createdAt >= :from)
            AND   (:to          IS NULL OR o.createdAt <= :to)
            ORDER BY o.createdAt ASC
            """)
    List<OutboxRecord> findPublishedForReplay(
            @Param("tenantId")   String  tenantId,
            @Param("eventType")  String  eventType,
            @Param("from")       Instant from,
            @Param("to")         Instant to
    );

    /**
     * Finds specific outbox records by ID for targeted replay.
     * Returned in DB natural order — caller sorts if ordering matters.
     */
    List<OutboxRecord> findAllByIdIn(List<UUID> ids);
}
