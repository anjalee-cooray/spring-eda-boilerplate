package com.example.eda.db;

import com.example.eda.db.eventstore.EventStoreWriter;
import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxRepository;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Load / stress tests for OutboxWriter under concurrent write pressure.
 *
 * These tests verify three guarantees that must hold regardless of write throughput:
 *
 *   1. Uniqueness — every write produces a distinct event_id (no UUID collisions or
 *      duplicate inserts that bypass the unique constraint)
 *
 *   2. Dual-write integrity — outbox_records and event_store counts must match after
 *      all concurrent writes complete (atomicity holds under concurrency)
 *
 *   3. Throughput floor — sequential writes must sustain at least 50 events/second
 *      on the CI Postgres container (a very conservative floor that would fail only
 *      if something is catastrophically wrong, e.g. every write creates a transaction
 *      round-trip that includes an N+1 query)
 *
 * Design:
 *   Each thread wraps its write in a TransactionTemplate so it gets its own committed
 *   transaction. @Transactional(NOT_SUPPORTED) on the test method ensures no outer
 *   test-managed transaction interferes. Cleanup runs in @AfterEach via JDBC DELETE.
 */
@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({OutboxWriter.class, EventStoreWriter.class, ObjectMapper.class})
@Tag("integration")
class OutboxWriterLoadTest {

    private static final int THREADS       = 10;
    private static final int EVENTS_PER_THREAD = 50; // 500 total per concurrent test

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

    @Autowired OutboxWriter outboxWriter;
    @Autowired OutboxRepository outboxRepository;
    @Autowired PlatformTransactionManager txManager;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void setTenantContext() {
        TenantContextHolder.set(new TenantContext("tenant-load", "load-subject", List.of("TENANT_ADMIN")));
    }

    @AfterEach
    void tearDown() {
        TenantContextHolder.clear();
        jdbc.execute("DELETE FROM event_store WHERE tenant_id = 'tenant-load'");
        jdbc.execute("DELETE FROM outbox_records WHERE tenant_id = 'tenant-load'");
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * 500 concurrent writes across 10 threads must produce 500 distinct event_ids.
     * Verifies: UUID generation uniqueness + DB unique constraint under concurrency.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrent500Writes_allProduceUniqueEventIds() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        List<Future<List<UUID>>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                List<UUID> ids = new ArrayList<>(EVENTS_PER_THREAD);
                TenantContextHolder.set(
                        new TenantContext("tenant-load", "load-subj-" + threadId, List.of("TENANT_ADMIN")));
                try {
                    for (int i = 0; i < EVENTS_PER_THREAD; i++) {
                        final int eventIdx = i;
                        UUID eventId = new TransactionTemplate(txManager).execute(status -> {
                            OutboxRecord record = outboxWriter.write(
                                    "example.created",
                                    Map.of("threadId", threadId, "eventIdx", eventIdx),
                                    "corr-" + threadId + "-" + eventIdx);
                            return record.getEventId();
                        });
                        ids.add(eventId);
                    }
                } finally {
                    TenantContextHolder.clear();
                }
                return ids;
            }));
        }

        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        List<UUID> allIds = new ArrayList<>();
        for (Future<List<UUID>> f : futures) {
            allIds.addAll(f.get());
        }

        int expected = THREADS * EVENTS_PER_THREAD;
        assertThat(allIds).hasSize(expected);
        assertThat(new HashSet<>(allIds)).as("all event_ids must be unique").hasSize(expected);
    }

    /**
     * After 500 concurrent writes, outbox_records count must equal event_store count.
     * Verifies: dual-write atomicity holds under concurrency — no write lands in one
     * table but not the other.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrent500Writes_dualWriteIntact_outboxMatchesEventStore() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        List<Future<Void>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                TenantContextHolder.set(
                        new TenantContext("tenant-load", "load-subj-" + threadId, List.of("TENANT_ADMIN")));
                try {
                    for (int i = 0; i < EVENTS_PER_THREAD; i++) {
                        final int eventIdx = i;
                        new TransactionTemplate(txManager).executeWithoutResult(status ->
                                outboxWriter.write(
                                        "example.created",
                                        Map.of("t", threadId, "i", eventIdx),
                                        "corr-" + threadId + "-" + eventIdx));
                    }
                } finally {
                    TenantContextHolder.clear();
                }
                return null;
            }));
        }

        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        for (Future<Void> f : futures) f.get(); // re-throw any exceptions from threads

        long outboxCount = outboxRepository.countByStatus(OutboxRecord.OutboxStatus.PENDING);
        Integer storeCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM event_store WHERE tenant_id = 'tenant-load'",
                Integer.class);

        assertThat(outboxCount)
                .as("outbox_records count must equal event_store count (dual-write atomicity)")
                .isEqualTo(storeCount);
        assertThat(outboxCount).isEqualTo((long) THREADS * EVENTS_PER_THREAD);
    }

    /**
     * Sequential writes must sustain at least 50 events/second.
     *
     * This is a very conservative floor — a healthy in-process DB call should easily
     * exceed 200 events/sec even on a low-powered CI runner with a Testcontainers DB.
     * If this fails, something is catastrophically wrong (e.g. N+1 queries on every write).
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void sequentialWrites_throughputExceedsFloor() {
        int eventCount = 100;
        TenantContextHolder.set(new TenantContext("tenant-load", "throughput-subj", List.of("TENANT_ADMIN")));

        long startNs = System.nanoTime();

        for (int i = 0; i < eventCount; i++) {
            final int idx = i;
            new TransactionTemplate(txManager).executeWithoutResult(status ->
                    outboxWriter.write("example.created", Map.of("i", idx), "corr-throughput-" + idx));
        }

        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
        double eventsPerSec = (double) eventCount / (elapsedMs / 1000.0);

        assertThat(eventsPerSec)
                .as("OutboxWriter throughput must exceed 50 events/sec (elapsed=%dms, rate=%.1f/sec)",
                        elapsedMs, eventsPerSec)
                .isGreaterThan(50.0);
    }
}
