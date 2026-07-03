package com.example.eda.command.saga;

import com.example.eda.events.envelope.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Saga skeleton — orchestrates multi-step flows triggered by domain events.
 * Replace this with real saga steps as business requirements grow.
 *
 * Pattern: each step publishes a command event; downstream services react and publish results.
 * Compensation: on failure, publish a compensating event to undo completed steps.
 */
@Component
public class ExampleSaga {

    private static final Logger log = LoggerFactory.getLogger(ExampleSaga.class);

    public void onExampleCreated(EventEnvelope envelope) {
        log.info("Saga step 1 — example.created received eventId={} tenantId={}",
                envelope.eventId(), envelope.tenantId());

        // TODO: implement saga step 1
        // e.g. publish a command to trigger the next step
    }

    public void onExampleActivated(EventEnvelope envelope) {
        log.info("Saga step 2 — example.activated received eventId={} tenantId={}",
                envelope.eventId(), envelope.tenantId());

        // TODO: implement saga step 2
    }

    public void compensate(EventEnvelope envelope, Exception reason) {
        log.error("Saga compensation triggered for eventId={} tenantId={} reason={}",
                envelope.eventId(), envelope.tenantId(), reason.getMessage());

        // TODO: implement compensating transactions
    }
}
