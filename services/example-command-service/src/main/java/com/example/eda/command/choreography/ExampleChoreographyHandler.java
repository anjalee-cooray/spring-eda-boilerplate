package com.example.eda.command.choreography;

import com.example.eda.command.domain.ExampleEntity;
import com.example.eda.command.domain.ExampleRepository;
import com.example.eda.db.outbox.OutboxWriter;
import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Demonstrates event choreography — this service reacts to its own published event
 * and produces the next event in the flow, with no central coordinator.
 *
 * Choreography flow:
 *   1. command-service publishes "example.created"          (ExampleCommandHandler)
 *   2. THIS handler receives "example.created" via broker
 *   3. Activates the entity and publishes "example.activated"
 *   4. Any subscriber (query-service, notification-service, etc.) reacts independently
 *
 * In a real system, step 2 would typically live in a separate downstream service
 * (e.g. a validation or enrichment service) that publishes the result back.
 * It lives here to keep the boilerplate self-contained and runnable as a single service.
 *
 * When to use choreography vs orchestration:
 *   Choreography  — services react to events independently; no central state tracking needed;
 *                   flows are simple and linear; loose coupling is the priority.
 *   Orchestration — flows are complex with branching, retries, and compensation across many
 *                   services; you need a saga state machine (saga_instances table) and a
 *                   framework like Axon or Conductor to track which step each saga is on.
 */
@Component
public class ExampleChoreographyHandler implements EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ExampleChoreographyHandler.class);

    private final ExampleRepository repository;
    private final OutboxWriter outboxWriter;

    public ExampleChoreographyHandler(ExampleRepository repository, OutboxWriter outboxWriter) {
        this.repository = repository;
        this.outboxWriter = outboxWriter;
    }

    @Override
    public boolean supports(String eventType) {
        return "example.created".equals(eventType);
    }

    @Override
    @Transactional
    public void handle(EventEnvelope envelope) {
        UUID entityId = UUID.fromString(
                envelope.payload().get("id").toString());

        ExampleEntity entity = repository.findById(entityId).orElseThrow(() ->
                new IllegalStateException("Entity not found for choreography step: " + entityId));

        entity.activate();
        repository.save(entity);

        outboxWriter.write(
                "example.activated",
                Map.of(
                        "id", entity.getId(),
                        "name", entity.getName(),
                        "tenantId", entity.getTenantId(),
                        "status", entity.getStatus().name()
                ),
                envelope.correlationId()
        );

        log.info("Choreography step complete — entity activated and example.activated published: id={} tenantId={}",
                entityId, envelope.tenantId());
    }
}
