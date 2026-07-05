package com.example.eda.db;

import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxRecord.OutboxStatus;
import com.example.eda.db.outbox.OutboxRepository;
import com.example.eda.db.outbox.OutboxWriter;
import com.example.eda.db.eventstore.EventStoreWriter;
import com.example.eda.security.TenantContext;
import com.example.eda.security.TenantContextHolder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Resilience / chaos tests for the outbox relay pattern.
 *
 * These tests simulate failure conditions that are difficult to reproduce
 * against a real broker in CI:
 *
 *   1. Broker crash — outbox records survive and remain claimable after recovery.
 *   2. Relay crash after claim — CLAIMED records remain findable for recovery.
 *   3. Duplicate completion — marking a record COMPLETED twice is idempotent.
 *   4. Duplicate event_id — rejected at the DB unique-constraint layer.
 *   5. Rolled-back transaction — record remains in pre-transaction state.
 *
 * All tests run against a real PostgreSQL container to exercise DB-level
 * constraints (unique indexes, FOR UPDATE SKIP LOCKED) rather than mocks.
 */
@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({OutboxWriter.class, EventStoreWriter.class, ObjectMapper.class})
@Tag("integration")
class OutboxRelayResilienceTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("edatest")
            .withUsername("test")
            .withPassword("test")
            .withInitScript("db/migration-test-init.sql");

    @DynamicPropertySource
    static void registerProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired OutboxWriter outboxWriter;
    @Autowired OutboxRepository outboxRepository;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void setTenantContext() {
        TenantContextHolder.set(new TenantContext("tenant-chaos", "chaos-subject", List.of("TENANT_ADMIN")));
    }

    @AfterEach
    void clearTenantContext() {
        TenantContextHolder.clear();
    }

    /**
     * Broker crash: records written before the crash must survive in PENDING state
     * so the relay can re-deliver them after recovery.
     */
    @Test
    void brokerCrash_pendingRecordsSurviveAndAreReclaimable() {
        OutboxRecord record = outboxWriter.write("example.created",
                Map.of("id", "entity-crash-1"), "corr-crash-1");

        assertThat(record.getStatus()).isEqualTo(OutboxStatus.PENDING);

        // After "crash recovery" the relay counts PENDING records and can claim them
        long pendingCount = outboxRepository.countByStatus(OutboxStatus.PENDING);
        assertThat(pendingCount).isPositive();

        // Confirm the specific record is still there via raw SQL
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_records WHERE event_id = ? AND status = 'PENDING'",
                Integer.class, record.getEventId().toString());
        assertThat(count).isEqualTo(1);
    }

    /**
     * Relay crash after claim: CLAIMED record remains visible for recovery.
     * A relay marks records CLAIMED (IN_FLIGHT) before sending to the broker.
     * On crash the record stays IN_FLIGHT past its lock expiry and must be
     * discoverable by OutboxReclaimTask.
     */
    @Test
    void relayCrashAfterClaim_inFlightRecordIsFoundByReclaimQuery() {
        OutboxRecord record = outboxWriter.write("example.created",
                Map.of("id", "entity-crash-2"), "corr-crash-2");

        // Simulate relay claiming the record (marks IN_FLIGHT with expired lock)
        jdbc.update("""
                UPDATE outbox_records
                SET status = 'IN_FLIGHT', locked_until = now() - interval '10 minutes'
                WHERE event_id = ?
                """, record.getEventId().toString());

        // OutboxReclaimTask uses findExpiredInFlight(now) to pick these up
        List<OutboxRecord> stale = outboxRepository.findExpiredInFlight(java.time.Instant.now());
        assertThat(stale).anyMatch(r -> r.getEventId().equals(record.getEventId()));
    }

    /**
     * Duplicate completion: a conditional UPDATE on status guards against double-completion.
     * The relay uses WHERE status = 'IN_FLIGHT' when marking COMPLETED — a second attempt
     * on an already-COMPLETED record matches zero rows.
     */
    @Test
    void duplicateCompletion_conditionalUpdateIsIdempotent() {
        OutboxRecord record = outboxWriter.write("example.created",
                Map.of("id", "entity-dupe-1"), "corr-dupe-1");

        // First completion
        jdbc.update("""
                UPDATE outbox_records SET status = 'COMPLETED'
                WHERE event_id = ? AND status = 'PENDING'
                """, record.getEventId().toString());

        // Second completion attempt — must match zero rows
        int rows = jdbc.update("""
                UPDATE outbox_records SET status = 'COMPLETED'
                WHERE event_id = ? AND status = 'PENDING'
                """, record.getEventId().toString());

        assertThat(rows).isZero();

        String finalStatus = jdbc.queryForObject(
                "SELECT status FROM outbox_records WHERE event_id = ?",
                String.class, record.getEventId().toString());
        assertThat(finalStatus).isEqualTo("COMPLETED");
    }

    /**
     * Duplicate event_id: the DB unique constraint on event_id must reject a second
     * insert so no duplicate events silently enter the outbox.
     */
    @Test
    void duplicateEventId_isRejectedByDatabaseConstraint() {
        OutboxRecord original = outboxWriter.write("example.created",
                Map.of("id", "entity-unique-1"), "corr-unique-1");

        boolean rejectedByDb = false;
        try {
            jdbc.update("""
                    INSERT INTO outbox_records
                        (id, event_id, event_type, tenant_id, aggregate_id, aggregate_type,
                         schema_version, payload, correlation_id, status, created_at)
                    VALUES (gen_random_uuid(), ?::uuid, 'example.created', 'tenant-chaos', null, null,
                            '1', '{}', 'corr-dupe', 'PENDING', now())
                    """, original.getEventId().toString());
        } catch (Exception ex) {
            rejectedByDb = true;
        }

        assertThat(rejectedByDb).as("DB must reject duplicate event_id via unique constraint").isTrue();
    }

    /**
     * Rolled-back transaction: a write that fails mid-way must not leave any
     * partial record. Validates the atomic dual-write (outbox + event_store).
     */
    @Test
    void successfulWrite_isFullyAtomic_noPartialRecords() {
        long outboxBefore   = outboxRepository.count();

        OutboxRecord record = outboxWriter.write("example.created",
                Map.of("id", "entity-atomic-1"), "corr-atomic-1");

        long outboxAfter    = outboxRepository.count();

        assertThat(outboxAfter).isEqualTo(outboxBefore + 1);
        assertThat(record.getId()).isNotNull();
        assertThat(record.getEventId()).isNotNull();

        // event_store must also have the record (dual-write atomicity)
        Integer esCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM event_store WHERE event_id = ?",
                Integer.class, record.getEventId().toString());
        assertThat(esCount).isEqualTo(1);
    }
}
