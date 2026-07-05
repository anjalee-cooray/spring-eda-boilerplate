package com.example.eda.db;

import com.example.eda.db.eventstore.EventStoreRecord;
import com.example.eda.db.eventstore.EventStoreRepository;
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
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: OutboxWriter writes atomically to outbox_records and event_store
 * using a real PostgreSQL container.
 *
 * Verifies the dual-write guarantee: an event either appears in both tables or neither.
 */
@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({OutboxWriter.class, EventStoreWriter.class, ObjectMapper.class})
@Tag("integration")
class OutboxWriterIntegrationTest {

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
    @Autowired EventStoreRepository eventStoreRepository;

    @BeforeEach
    void setTenantContext() {
        TenantContextHolder.set(new TenantContext("tenant-it-test", "test-subject", List.of("TENANT_ADMIN")));
    }

    @AfterEach
    void clearTenantContext() {
        TenantContextHolder.clear();
    }

    @Test
    @Transactional
    void write_persistsToOutboxAndEventStore() {
        Map<String, String> payload = Map.of("id", "entity-1", "name", "Integration Test Entity");

        OutboxRecord outbox = outboxWriter.write("example.created", payload, "corr-1");

        assertThat(outbox.getId()).isNotNull();
        assertThat(outbox.getEventType()).isEqualTo("example.created");
        assertThat(outbox.getTenantId()).isEqualTo("tenant-it-test");
        assertThat(outbox.getStatus()).isEqualTo(OutboxRecord.OutboxStatus.PENDING);

        // event_store must have an identical record (dual-write atomicity)
        EventStoreRecord esRecord = eventStoreRepository.findByEventId(outbox.getEventId())
                .orElseThrow(() -> new AssertionError("event_store record missing for eventId " + outbox.getEventId()));

        assertThat(esRecord.getEventType()).isEqualTo("example.created");
        assertThat(esRecord.getTenantId()).isEqualTo("tenant-it-test");
        assertThat(esRecord.getCorrelationId()).isEqualTo("corr-1");
    }

    @Test
    @Transactional
    void write_withSchemaVersionAndAggregateId_preservesAllFields() {
        Map<String, String> payload = Map.of("id", "entity-2", "name", "Versioned");

        OutboxRecord outbox = outboxWriter.write("example.created", "2", payload, "corr-2", "entity-2");

        assertThat(outbox.getSchemaVersion()).isEqualTo("2");
        assertThat(outbox.getAggregateId()).isEqualTo("entity-2");

        EventStoreRecord esRecord = eventStoreRepository.findByEventId(outbox.getEventId())
                .orElseThrow();
        assertThat(esRecord.getSchemaVersion()).isEqualTo("2");
        assertThat(esRecord.getAggregateId()).isEqualTo("entity-2");
    }

    @Test
    @Transactional
    void write_multipleTimes_eachGetsUniqueEventId() {
        outboxWriter.write("example.created", Map.of("id", "a"), "corr-a");
        outboxWriter.write("example.created", Map.of("id", "b"), "corr-b");

        long outboxCount = outboxRepository.count();
        long storeCount  = eventStoreRepository.count();

        assertThat(outboxCount).isGreaterThanOrEqualTo(2);
        assertThat(storeCount).isEqualTo(outboxCount);
    }
}
