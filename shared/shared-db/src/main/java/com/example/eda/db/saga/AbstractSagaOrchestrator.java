package com.example.eda.db.saga;

import com.example.eda.db.outbox.OutboxWriter;
import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Optional;

/**
 * Base class for DB-backed saga orchestrators.
 *
 * Eliminates the boilerplate present in every hand-rolled saga:
 *   - Loading saga instance by correlationId
 *   - Terminal state guard (don't process events for completed/compensated sagas)
 *   - Context serialisation / deserialisation
 *   - Consistent log structure
 *
 * Usage — extend this class and implement the abstract methods:
 *
 * <pre>{@code
 * @Component
 * public class OrderFulfillmentSaga extends AbstractSagaOrchestrator {
 *
 *     static final String SAGA_TYPE = "OrderFulfillmentSaga";
 *
 *     @Override public String sagaType()    { return SAGA_TYPE; }
 *     @Override public boolean supports(String t) { return "order.placed".equals(t) || ...; }
 *
 *     @Override
 *     @Transactional
 *     public void handle(EventEnvelope envelope) {
 *         switch (envelope.eventType()) {
 *             case "order.placed"      -> start(envelope);
 *             case "payment.confirmed" -> onPaymentConfirmed(envelope);
 *             ...
 *         }
 *     }
 * }
 * }</pre>
 *
 * Timeout detection:
 *   SagaTimeoutTask runs on a schedule and calls checkTimeouts(). For each
 *   STARTED saga older than the configured TTL it fires a synthetic compensation
 *   event so the saga can self-heal without manual operator intervention.
 *
 * Migrating to Temporal or Axon:
 *   For flows with 10+ steps, external HTTP calls, or days-long waits, replace
 *   this hand-rolled orchestrator with a framework:
 *   - Temporal: annotate workflow methods; Temporal handles durable execution,
 *     retries, timeouts, and visibility natively.
 *   - Axon Framework: SagaLifecycle + @SagaEventHandler mirrors this pattern
 *     exactly but adds distributed locking and saga versioning.
 *   The EventEnvelope contract and OutboxWriter are unchanged — only the
 *   orchestrator class needs replacing.
 */
public abstract class AbstractSagaOrchestrator implements EventConsumer {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final SagaRepository sagaRepository;
    protected final OutboxWriter outboxWriter;
    protected final ObjectMapper objectMapper;

    protected AbstractSagaOrchestrator(
            SagaRepository sagaRepository,
            OutboxWriter outboxWriter,
            ObjectMapper objectMapper) {
        this.sagaRepository = sagaRepository;
        this.outboxWriter   = outboxWriter;
        this.objectMapper   = objectMapper;
    }

    /** The unique name for this saga type, stored in saga_instances.saga_type. */
    public abstract String sagaType();

    /**
     * Loads a saga by correlationId. Returns empty if not found (late duplicate).
     * Returns the saga regardless of terminal state — caller must guard with isTerminal().
     */
    protected Optional<SagaInstance> load(String correlationId) {
        Optional<SagaInstance> opt = sagaRepository.findByCorrelationId(correlationId);
        if (opt.isEmpty()) {
            log.warn("Saga not found correlationId={} sagaType={} — may be late duplicate",
                    correlationId, sagaType());
        }
        return opt;
    }

    /**
     * Loads a saga and returns null if not found or already terminal.
     * Use this in step handlers to skip processing with a single null-check.
     */
    protected SagaInstance loadActive(String correlationId) {
        return load(correlationId).filter(s -> {
            if (s.isTerminal()) {
                log.warn("Event arrived for terminal saga correlationId={} status={}",
                        correlationId, s.getStatus());
                return false;
            }
            return true;
        }).orElse(null);
    }

    /** Serialises the context map to JSON for storage in saga_instances.context. */
    protected String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialise saga context", e);
        }
    }

    /** Deserialises saga_instances.context back to a Map. */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> fromJson(String json) {
        try {
            return objectMapper.readValue(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialise saga context", e);
        }
    }

    /** Extracts a field from the event payload safely. Returns "" if missing. */
    @SuppressWarnings("unchecked")
    protected Object extractField(Object payload, String field) {
        if (payload instanceof Map<?, ?> m) {
            return m.getOrDefault(field, "");
        }
        return "";
    }
}
