package com.example.eda.consumer.saga;

import com.example.eda.db.outbox.OutboxWriter;
import com.example.eda.db.saga.SagaInstance;
import com.example.eda.db.saga.SagaRepository;
import com.example.eda.db.saga.SagaStatus;
import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Saga orchestrator for the example booking flow.
 *
 * This class demonstrates the DB-backed saga orchestrator pattern — an alternative
 * to pure choreography for flows that require compensation, branching, or long-running
 * coordination across many services.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * CHOREOGRAPHY vs ORCHESTRATION — when to use which
 *
 *   Choreography (existing pattern in this boilerplate):
 *     Each service reacts to events independently. No central coordinator.
 *     Good for: simple flows, loose coupling, few steps.
 *
 *   Orchestration (this class):
 *     One component (the saga) explicitly tells each participant what to do next.
 *     Good for: compensation logic, branching, 3+ services, long-running flows.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * FLOW
 *
 *   Trigger → booking.requested
 *
 *     Step 1: publish inventory.reserve
 *       ├── booking.inventory.confirmed  → Step 2
 *       └── booking.inventory.failed    → COMPENSATED (nothing to undo)
 *
 *     Step 2: publish payment.charge
 *       ├── booking.payment.confirmed   → COMPLETED
 *       └── booking.payment.failed      → COMPENSATING
 *
 *   Compensation (reverse order):
 *     Comp-0: publish inventory.release
 *       └── booking.inventory.released  → COMPENSATED
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * CRASH SAFETY
 *
 *   All state transitions (saga status, current step, context) are written to
 *   saga_instances in the same transaction as the outbox record for the next
 *   command event. If the service crashes after commit, the next response event
 *   reloads the saga from DB by correlation_id and continues from exactly where
 *   it left off.
 *
 *   If the service crashes before commit, both the state update and the outbox
 *   record are rolled back together — no orphaned saga step, no duplicate command.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * HOW TO ADD A NEW SAGA
 *
 *   1. Add saga-specific event types (trigger + step responses + compensation responses)
 *   2. Create a class that implements EventConsumer (like this one)
 *   3. In start(): create SagaInstance, write first command to outbox, save saga
 *   4. In handle(): route by eventType and correlationId, call the right step handler
 *   5. Each step handler: update saga state + write next command to outbox in one tx
 *   6. Compensation handlers mirror the same pattern in reverse
 *
 * For complex flows (10+ steps, external system calls) consider a dedicated framework
 * such as Temporal or Axon instead of this hand-rolled approach.
 */
@Component
public class ExampleBookingSaga implements EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ExampleBookingSaga.class);

    static final String SAGA_TYPE = "ExampleBookingSaga";

    // Forward steps
    static final String STEP_AWAITING_INVENTORY  = "AWAITING_INVENTORY";
    static final String STEP_AWAITING_PAYMENT    = "AWAITING_PAYMENT";

    // Compensation steps
    static final String COMP_AWAITING_INVENTORY_RELEASE = "COMP_AWAITING_INVENTORY_RELEASE";

    // Event types this saga emits (commands to participants)
    static final String CMD_INVENTORY_RESERVE = "booking.inventory.reserve";
    static final String CMD_PAYMENT_CHARGE    = "booking.payment.charge";
    static final String CMD_INVENTORY_RELEASE = "booking.inventory.release";

    // Event types this saga listens for (responses from participants)
    static final String EVT_BOOKING_REQUESTED        = "booking.requested";
    static final String EVT_INVENTORY_CONFIRMED      = "booking.inventory.confirmed";
    static final String EVT_INVENTORY_FAILED         = "booking.inventory.failed";
    static final String EVT_PAYMENT_CONFIRMED        = "booking.payment.confirmed";
    static final String EVT_PAYMENT_FAILED           = "booking.payment.failed";
    static final String EVT_INVENTORY_RELEASED       = "booking.inventory.released";

    private final SagaRepository sagaRepository;
    private final OutboxWriter outboxWriter;
    private final ObjectMapper objectMapper;

    public ExampleBookingSaga(
            SagaRepository sagaRepository,
            OutboxWriter outboxWriter,
            ObjectMapper objectMapper) {
        this.sagaRepository = sagaRepository;
        this.outboxWriter = outboxWriter;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean supports(String eventType) {
        return EVT_BOOKING_REQUESTED.equals(eventType)
                || EVT_INVENTORY_CONFIRMED.equals(eventType)
                || EVT_INVENTORY_FAILED.equals(eventType)
                || EVT_PAYMENT_CONFIRMED.equals(eventType)
                || EVT_PAYMENT_FAILED.equals(eventType)
                || EVT_INVENTORY_RELEASED.equals(eventType);
    }

    /**
     * Routes incoming events to the correct step handler.
     * All transitions are @Transactional — saga state update and next outbox record
     * commit together or not at all.
     */
    @Override
    @Transactional
    public void handle(EventEnvelope envelope) {
        switch (envelope.eventType()) {
            case EVT_BOOKING_REQUESTED   -> start(envelope);
            case EVT_INVENTORY_CONFIRMED -> onInventoryConfirmed(envelope);
            case EVT_INVENTORY_FAILED    -> onInventoryFailed(envelope);
            case EVT_PAYMENT_CONFIRMED   -> onPaymentConfirmed(envelope);
            case EVT_PAYMENT_FAILED      -> onPaymentFailed(envelope);
            case EVT_INVENTORY_RELEASED  -> onInventoryReleased(envelope);
            default -> log.warn("ExampleBookingSaga received unexpected event type={}", envelope.eventType());
        }
    }

    // ── Step: start (triggered by booking.requested) ──────────────────────

    private void start(EventEnvelope envelope) {
        String correlationId = envelope.correlationId();

        if (sagaRepository.findByCorrelationId(correlationId).isPresent()) {
            log.warn("Saga already exists for correlationId={} — duplicate trigger event ignored", correlationId);
            return;
        }

        String context = toJson(Map.of(
                "bookingId",  extractField(envelope.payload(), "bookingId"),
                "tenantId",   envelope.tenantId(),
                "customerId", extractField(envelope.payload(), "customerId"),
                "itemId",     extractField(envelope.payload(), "itemId"),
                "amount",     extractField(envelope.payload(), "amount")
        ));

        SagaInstance saga = SagaInstance.builder()
                .sagaType(SAGA_TYPE)
                .correlationId(correlationId)
                .currentStep(STEP_AWAITING_INVENTORY)
                .context(context)
                .build();

        sagaRepository.save(saga);

        // Write the inventory reserve command to the outbox in the same transaction.
        // If this transaction rolls back, neither the saga nor the command exists — no orphans.
        outboxWriter.write(
                CMD_INVENTORY_RESERVE,
                Map.of("itemId", extractField(envelope.payload(), "itemId"),
                       "bookingId", extractField(envelope.payload(), "bookingId")),
                correlationId
        );

        log.info("Saga started type={} correlationId={} step={}", SAGA_TYPE, correlationId, STEP_AWAITING_INVENTORY);
    }

    // ── Step 1 response: inventory confirmed → charge payment ─────────────

    private void onInventoryConfirmed(EventEnvelope envelope) {
        SagaInstance saga = loadByCorrelation(envelope.correlationId());
        if (saga == null || saga.isTerminal()) return;

        saga.advanceTo(STEP_AWAITING_PAYMENT);
        sagaRepository.save(saga);

        Map<?, ?> ctx = fromJson(saga.getContext());
        outboxWriter.write(
                CMD_PAYMENT_CHARGE,
                Map.of("customerId", ctx.get("customerId"),
                       "amount",     ctx.get("amount"),
                       "bookingId",  ctx.get("bookingId")),
                envelope.correlationId()
        );

        log.info("Saga step correlationId={} inventoryConfirmed → step={}", envelope.correlationId(), STEP_AWAITING_PAYMENT);
    }

    // ── Step 1 failure: inventory failed → nothing to compensate, done ───

    private void onInventoryFailed(EventEnvelope envelope) {
        SagaInstance saga = loadByCorrelation(envelope.correlationId());
        if (saga == null || saga.isTerminal()) return;

        // Inventory was never reserved so no compensation is needed
        saga.markCompensated();
        sagaRepository.save(saga);

        log.info("Saga completed (inventory unavailable, no compensation) correlationId={}", envelope.correlationId());
    }

    // ── Step 2 response: payment confirmed → saga complete ────────────────

    private void onPaymentConfirmed(EventEnvelope envelope) {
        SagaInstance saga = loadByCorrelation(envelope.correlationId());
        if (saga == null || saga.isTerminal()) return;

        saga.complete();
        sagaRepository.save(saga);

        log.info("Saga COMPLETED correlationId={}", envelope.correlationId());
    }

    // ── Step 2 failure: payment failed → compensate (release inventory) ──

    private void onPaymentFailed(EventEnvelope envelope) {
        SagaInstance saga = loadByCorrelation(envelope.correlationId());
        if (saga == null || saga.isTerminal()) return;

        saga.startCompensation("Payment failed: " + extractField(envelope.payload(), "reason"));
        saga.advanceTo(COMP_AWAITING_INVENTORY_RELEASE);
        sagaRepository.save(saga);

        Map<?, ?> ctx = fromJson(saga.getContext());
        outboxWriter.write(
                CMD_INVENTORY_RELEASE,
                Map.of("itemId", ctx.get("itemId"), "bookingId", ctx.get("bookingId")),
                envelope.correlationId()
        );

        log.info("Saga COMPENSATING correlationId={} reason=paymentFailed", envelope.correlationId());
    }

    // ── Compensation response: inventory released → saga compensated ──────

    private void onInventoryReleased(EventEnvelope envelope) {
        SagaInstance saga = loadByCorrelation(envelope.correlationId());
        if (saga == null || saga.isTerminal()) return;

        saga.markCompensated();
        sagaRepository.save(saga);

        log.info("Saga COMPENSATED correlationId={}", envelope.correlationId());
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private SagaInstance loadByCorrelation(String correlationId) {
        Optional<SagaInstance> opt = sagaRepository.findByCorrelationId(correlationId);
        if (opt.isEmpty()) {
            log.warn("Saga not found for correlationId={} — event may be a late duplicate", correlationId);
            return null;
        }
        SagaInstance saga = opt.get();
        if (saga.isTerminal()) {
            log.warn("Event arrived for terminal saga correlationId={} status={}", correlationId, saga.getStatus());
        }
        return saga;
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialise saga context", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fromJson(String json) {
        try {
            return objectMapper.readValue(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialise saga context", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Object extractField(Object payload, String field) {
        if (payload instanceof Map<?, ?> m) {
            return m.getOrDefault(field, "");
        }
        return "";
    }
}
