package com.example.eda.db.health;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tracks consumer processing health after each successful handle() call.
 *
 * Updates consumer_offsets and registers a Micrometer gauge
 * (consumer.lag.seconds) per consumer/event-type/tenant combination.
 *
 * Usage — call inside the handler's @Transactional after markProcessed():
 *
 *   @Transactional
 *   public void handle(EventEnvelope envelope) {
 *       if (deduplicator.isDuplicate(envelope.eventId())) return;
 *       // ... business logic ...
 *       deduplicator.markProcessed(envelope.eventId(), envelope.eventType(), envelope.tenantId());
 *       healthTracker.recordSuccess(getClass().getSimpleName(), envelope);
 *   }
 *
 * The gauge `consumer.lag.seconds{consumer, event_type, tenant}` reports
 * how many seconds ago the last event was processed. An alert fires when
 * this exceeds your SLA threshold (e.g. > 300s with events still incoming).
 */
@Component
public class ConsumerHealthTracker {

    private final ConsumerOffsetRepository offsetRepository;
    private final MeterRegistry meterRegistry;

    // Tracks which gauges have already been registered to avoid duplicate registration
    private final Map<String, Boolean> registeredGauges = new ConcurrentHashMap<>();

    public ConsumerHealthTracker(ConsumerOffsetRepository offsetRepository, MeterRegistry meterRegistry) {
        this.offsetRepository = offsetRepository;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Records a successful event processing. Must be called inside a @Transactional
     * method so the offset update commits with the business logic.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void recordSuccess(String consumerName, UUID eventId, String eventType, String tenantId) {
        ConsumerOffset offset = offsetRepository
                .findByConsumerNameAndEventTypeAndTenantId(consumerName, eventType, tenantId)
                .orElseGet(() -> ConsumerOffset.builder()
                        .consumerName(consumerName)
                        .eventType(eventType)
                        .tenantId(tenantId)
                        .build());

        offset.recordSuccess(eventId, Instant.now());
        offsetRepository.save(offset);

        registerLagGaugeIfAbsent(consumerName, eventType, tenantId);
    }

    /**
     * Records a handler error (does not update the success offset).
     * Call inside a @Transactional catch block.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void recordError(String consumerName, String eventType, String tenantId, String errorMessage) {
        ConsumerOffset offset = offsetRepository
                .findByConsumerNameAndEventTypeAndTenantId(consumerName, eventType, tenantId)
                .orElseGet(() -> ConsumerOffset.builder()
                        .consumerName(consumerName)
                        .eventType(eventType)
                        .tenantId(tenantId)
                        .build());

        offset.recordError(errorMessage);
        offsetRepository.save(offset);
    }

    private void registerLagGaugeIfAbsent(String consumerName, String eventType, String tenantId) {
        String key = consumerName + "#" + eventType + "#" + tenantId;
        registeredGauges.computeIfAbsent(key, k -> {
            // Gauge reports seconds since the last successfully processed event.
            // Value is 0 when no event has been processed yet.
            meterRegistry.gauge(
                    "consumer.lag.seconds",
                    Tags.of("consumer", consumerName, "event_type", eventType, "tenant", tenantId),
                    offsetRepository,
                    repo -> repo.findByConsumerNameAndEventTypeAndTenantId(consumerName, eventType, tenantId)
                            .map(o -> o.getLastEventAt() != null
                                    ? (double) java.time.Duration.between(o.getLastEventAt(), Instant.now()).toSeconds()
                                    : 0.0)
                            .orElse(0.0)
            );
            return true;
        });
    }
}
