package com.example.eda.consumer.health;

import com.example.eda.db.health.ConsumerOffset;
import com.example.eda.db.health.ConsumerOffsetRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operator endpoint for consumer health inspection.
 *
 * GET /actuator/consumer-health
 *   Returns each consumer's last processed event, how long ago it was processed,
 *   and total events processed. Use this to spot stuck or lagging consumers.
 *
 * In Grafana, the consumer.lag.seconds gauge (registered by ConsumerHealthTracker)
 * gives the same information as a time-series dashboard. This endpoint provides
 * a human-readable snapshot for ad-hoc operator queries.
 *
 * Alert rule (Prometheus):
 *   consumer_lag_seconds{consumer="ExampleCreatedHandler"} > 300
 *   → consumer hasn't processed an event in 5 minutes
 */
@RestController
@RequestMapping("/actuator/consumer-health")
public class ConsumerHealthEndpoint {

    private final ConsumerOffsetRepository offsetRepository;

    public ConsumerHealthEndpoint(ConsumerOffsetRepository offsetRepository) {
        this.offsetRepository = offsetRepository;
    }

    @GetMapping
    public List<Map<String, Object>> health() {
        return offsetRepository.findAllByOrderByLastEventAtAsc().stream()
                .map(this::toView)
                .toList();
    }

    private Map<String, Object> toView(ConsumerOffset offset) {
        long lagSeconds = offset.getLastEventAt() != null
                ? Duration.between(offset.getLastEventAt(), Instant.now()).toSeconds()
                : -1;

        return Map.of(
                "consumer",        offset.getConsumerName(),
                "eventType",       offset.getEventType(),
                "tenantId",        offset.getTenantId(),
                "lastEventId",     offset.getLastEventId() != null ? offset.getLastEventId().toString() : "none",
                "lastEventAt",     offset.getLastEventAt() != null ? offset.getLastEventAt().toString() : "never",
                "lagSeconds",      lagSeconds,
                "eventsProcessed", offset.getEventsProcessed(),
                "lastError",       offset.getLastError() != null ? offset.getLastError() : "none"
        );
    }
}
