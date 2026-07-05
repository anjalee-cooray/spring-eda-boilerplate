package com.example.eda.db;

import com.example.eda.db.inbox.InboxDeduplicator;
import com.example.eda.db.inbox.InboxRepository;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: InboxDeduplicator correctly detects duplicate event IDs
 * using a real PostgreSQL container with a UNIQUE constraint on event_id.
 */
@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(InboxDeduplicator.class)
@Tag("integration")
class InboxDeduplicatorIntegrationTest {

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

    @Autowired InboxDeduplicator deduplicator;
    @Autowired InboxRepository inboxRepository;

    @Test
    @Transactional
    void firstSeen_isNotDuplicate_thenAfterMark_isDuplicate() {
        UUID eventId = UUID.randomUUID();

        assertThat(deduplicator.isDuplicate(eventId)).isFalse();

        deduplicator.markProcessed(eventId, "example.created", "tenant-it");

        assertThat(deduplicator.isDuplicate(eventId)).isTrue();
    }

    @Test
    @Transactional
    void differentEventIds_neverCollide() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        deduplicator.markProcessed(id1, "example.created", "tenant-it");

        assertThat(deduplicator.isDuplicate(id1)).isTrue();
        assertThat(deduplicator.isDuplicate(id2)).isFalse();
    }

    @Test
    @Transactional
    void sameEventId_differentTenants_stillDuplicate() {
        // Deduplication is by event_id only — tenant_id is not part of the key.
        // An event_id is globally unique (UUID), so cross-tenant collision is impossible
        // in practice, but the schema does not enforce per-tenant uniqueness intentionally.
        UUID eventId = UUID.randomUUID();

        deduplicator.markProcessed(eventId, "example.created", "tenant-A");

        assertThat(deduplicator.isDuplicate(eventId)).isTrue();
    }
}
