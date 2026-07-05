package com.example.eda.db.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxRecord, UUID> {

    /**
     * Selects IDs of PENDING records with a row-level FOR UPDATE SKIP LOCKED.
     *
     * SKIP LOCKED means rows already locked by another relay instance (i.e. in the
     * process of being claimed) are silently skipped rather than causing a block or
     * an error. This makes it safe to run multiple relay instances simultaneously —
     * each instance claims a disjoint set of records.
     *
     * Must be called inside a transaction — the locks are held until that transaction
     * commits or rolls back. The caller (OutboxClaimService) immediately follows with
     * markInFlight() in the same transaction so the records transition to IN_FLIGHT
     * before the locks are released.
     *
     * Returns List<Object> because native queries return raw JDBC values; the caller
     * maps each element to UUID.
     */
    @Query(value = """
            SELECT id FROM outbox_records
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Object> findPendingIdsForUpdate(@Param("limit") int limit);

    /**
     * Bulk-updates a batch of records from PENDING → IN_FLIGHT.
     *
     * clearAutomatically = true evicts the updated entities from Hibernate's
     * first-level cache so the subsequent findAllById() returns fresh state
     * (IN_FLIGHT, with the new locked_until and incremented attempt_count).
     */
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE OutboxRecord o
            SET o.status       = 'IN_FLIGHT',
                o.lockedUntil  = :lockedUntil,
                o.attemptCount = o.attemptCount + 1
            WHERE o.id IN :ids
            """)
    void markInFlight(@Param("ids") List<UUID> ids, @Param("lockedUntil") Instant lockedUntil);

    /**
     * Returns IN_FLIGHT records whose lock has expired (locked_until < now).
     * Used by OutboxReclaimTask to find records orphaned by a crashed relay.
     */
    @Query("SELECT o FROM OutboxRecord o WHERE o.status = 'IN_FLIGHT' AND o.lockedUntil < :now")
    List<OutboxRecord> findExpiredInFlight(@Param("now") Instant now);

    /** Count records by status — used for Micrometer gauges and alerting. */
    long countByStatus(OutboxRecord.OutboxStatus status);

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

    /**
     * Counts outbox records for a tenant created on or after a given timestamp.
     * Used by TenantQuotaEnforcer to measure today's write volume at domain level.
     * The (tenant_id, created_at) index makes this O(log N).
     */
    long countByTenantIdAndCreatedAtAfter(String tenantId, java.time.Instant createdAt);
}
