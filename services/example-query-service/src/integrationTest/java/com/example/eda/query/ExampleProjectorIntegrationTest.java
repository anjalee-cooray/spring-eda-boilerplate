package com.example.eda.query;

import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.query.projection.ExampleProjector;
import com.example.eda.query.projection.ExampleReadModel;
import com.example.eda.query.projection.ExampleReadModelRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: ExampleProjector persists a read-model row from an example.created event
 * using a real PostgreSQL container.
 *
 * This test catches projection bugs that unit tests miss — e.g. wrong column mapping,
 * missing NOT NULL constraint, or payload deserialization issues.
 */
@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({ExampleProjector.class, ObjectMapper.class})
@Tag("integration")
class ExampleProjectorIntegrationTest {

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

    @Autowired ExampleProjector projector;
    @Autowired ExampleReadModelRepository readModelRepository;

    @Test
    @Transactional
    void handle_exampleCreated_persistsReadModel() {
        UUID entityId = UUID.randomUUID();
        Map<String, String> payload = Map.of("id", entityId.toString(), "name", "Integration Widget");

        EventEnvelope envelope = EventEnvelope.builder()
                .eventType("example.created")
                .tenantId("tenant-it")
                .correlationId("corr-it")
                .payload(payload)
                .build();

        projector.handle(envelope);

        Optional<ExampleReadModel> found = readModelRepository.findById(entityId);
        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("Integration Widget");
        assertThat(found.get().getTenantId()).isEqualTo("tenant-it");
        assertThat(found.get().getStatus()).isEqualTo("CREATED");
    }

    @Test
    @Transactional
    void handle_unrelatedEventType_isIgnored() {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventType("payment.completed")
                .tenantId("tenant-it")
                .correlationId("corr-x")
                .payload(Map.of("id", UUID.randomUUID().toString()))
                .build();

        // supports() returns false — projector should not be called
        assertThat(projector.supports("payment.completed")).isFalse();
        // Confirm no rows written
        assertThat(readModelRepository.count()).isZero();
    }

    @Test
    @Transactional
    void handle_multipleEvents_createsDistinctRows() {
        for (int i = 0; i < 3; i++) {
            UUID id = UUID.randomUUID();
            projector.handle(EventEnvelope.builder()
                    .eventType("example.created")
                    .tenantId("tenant-it")
                    .correlationId("corr-" + i)
                    .payload(Map.of("id", id.toString(), "name", "Widget " + i))
                    .build());
        }

        assertThat(readModelRepository.count()).isEqualTo(3);
    }
}
