package com.example.eda.db;

import com.example.eda.db.eventstore.EventStoreWriter;
import com.example.eda.db.inbox.InboxDeduplicator;
import com.example.eda.db.outbox.OutboxWriter;
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
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that InboxDeduplicator correctly guards against double-processing
 * when a DLQ message is re-driven back to the source queue.
 *
 * The DLQ scenario that these tests protect:
 *   1. Event arrives → consumer processes it → markProcessed() records the event_id in inbox
 *   2. A bug, network blip, or operator re-drive causes the same event to arrive again
 *   3. isDuplicate() must return true → consumer skips the event entirely
 *   4. No double-processing of state-changing operations (charges, inventory, etc.)
 *
 * Why a real DB matters here:
 *   The unique constraint on inbox_records.event_id is the final safety net.
 *   Unit tests with mocked repositories cannot verify this constraint holds.
 *   These tests run against a real PostgreSQL container to prove it.
 *
 * Transaction note:
 *   isDuplicate() runs in REQUIRES_NEW (always commits, always sees committed data).
 *   markProcessed() runs in MANDATORY (joins the caller's transaction).
 *   Tests use @Transactional(NOT_SUPPORTED) + TransactionTemplate to commit
 *   markProcessed() before asserting, matching the production call pattern.
 */
@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({OutboxWriter.class, EventStoreWriter.class, InboxDeduplicator.class, ObjectMapper.class})
@Tag("integration")
class DlqRecoveryIdempotencyTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("edatest")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void registerProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired InboxDeduplicator inboxDeduplicator;
    @Autowired PlatformTransactionManager txManager;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void setTenantContext() {
        TenantContextHolder.set(new TenantContext("tenant-dlq", "dlq-subject", List.of("TENANT_ADMIN")));
    }

    @AfterEach
    void tearDown() {
        TenantContextHolder.clear();
        jdbc.execute("DELETE FROM inbox_records WHERE tenant_id = 'tenant-dlq'");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void commitMarkProcessed(UUID eventId) {
        new TransactionTemplate(txManager).executeWithoutResult(status ->
                inboxDeduplicator.markProcessed(eventId, "example.created", "tenant-dlq"));
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * First time processing: isDuplicate returns false — event is new and may be processed.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void firstProcessing_eventIsNotYetDuplicate() {
        UUID eventId = UUID.randomUUID();

        assertThat(inboxDeduplicator.isDuplicate(eventId)).isFalse();
    }

    /**
     * After markProcessed commits, isDuplicate returns true on the same event_id.
     * This is the core DLQ re-drive guard.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void afterMarkProcessed_sameEventId_isRecognisedAsDuplicate() {
        UUID eventId = UUID.randomUUID();

        commitMarkProcessed(eventId);

        // DLQ re-drive: same event arrives again — must be detected as duplicate
        assertThat(inboxDeduplicator.isDuplicate(eventId)).isTrue();
    }

    /**
     * Full DLQ re-drive scenario end-to-end:
     *   Step 1 — Normal processing: isDuplicate=false, then markProcessed
     *   Step 2 — DLQ re-drive: same event_id arrives again, isDuplicate=true
     *   Step 3 — Consumer skips the event (double-processing avoided)
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void dlqRedrive_fullScenario_duplicateDetectedBeforeProcessing() {
        UUID eventId = UUID.randomUUID();

        // Step 1: normal first-time arrival
        assertThat(inboxDeduplicator.isDuplicate(eventId)).isFalse();
        commitMarkProcessed(eventId); // consumer processes the event

        // Step 2: DLQ re-drive — same event arrives again
        boolean isDuplicate = inboxDeduplicator.isDuplicate(eventId);

        // Step 3: consumer sees duplicate=true and skips (no further processing)
        assertThat(isDuplicate).isTrue();
    }

    /**
     * Two different events on the same tenant are independent — neither is a duplicate
     * of the other even if they share tenantId or aggregateId.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void differentEventIds_onSameTenant_neitherIsDuplicate() {
        UUID eventId1 = UUID.randomUUID();
        UUID eventId2 = UUID.randomUUID();

        commitMarkProcessed(eventId1); // event 1 processed

        assertThat(inboxDeduplicator.isDuplicate(eventId1)).isTrue();  // processed ✓
        assertThat(inboxDeduplicator.isDuplicate(eventId2)).isFalse(); // different event, not a duplicate
    }

    /**
     * The DB unique constraint on event_id is the final safety net:
     * even if application code calls markProcessed twice (a bug), the DB rejects it.
     * This proves the constraint exists and is enforced at the database layer.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void markProcessedTwice_uniqueConstraintPreventsDoubleInsert() {
        UUID eventId = UUID.randomUUID();

        commitMarkProcessed(eventId); // first processing — succeeds

        // Second markProcessed on the same event_id must fail at the DB constraint level
        boolean constraintViolationThrown = false;
        try {
            commitMarkProcessed(eventId); // should fail with unique constraint violation
        } catch (Exception ex) {
            constraintViolationThrown = true;
        }

        assertThat(constraintViolationThrown)
                .as("DB unique constraint on event_id must prevent double-markProcessed").isTrue();
    }

    /**
     * Multiple events from the same DLQ topic are deduped independently:
     * marking event A as processed does not affect event B.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void multipleEventsFromDlq_eachDedupedIndependently() {
        UUID eventIdA = UUID.randomUUID();
        UUID eventIdB = UUID.randomUUID();
        UUID eventIdC = UUID.randomUUID();

        // A and B processed; C never processed
        commitMarkProcessed(eventIdA);
        commitMarkProcessed(eventIdB);

        assertThat(inboxDeduplicator.isDuplicate(eventIdA)).isTrue();
        assertThat(inboxDeduplicator.isDuplicate(eventIdB)).isTrue();
        assertThat(inboxDeduplicator.isDuplicate(eventIdC)).isFalse();
    }
}
