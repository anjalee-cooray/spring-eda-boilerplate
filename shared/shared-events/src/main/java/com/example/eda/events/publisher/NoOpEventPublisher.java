package com.example.eda.events.publisher;

import com.example.eda.events.envelope.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

/**
 * Fallback publisher used when no broker is configured (app.events.broker is unset).
 * Events are logged and discarded — safe for early-stage development when only
 * the command service is running and no async consumers exist yet.
 */
@Component
@ConditionalOnMissingBean(value = EventPublisher.class, ignored = NoOpEventPublisher.class)
public class NoOpEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(NoOpEventPublisher.class);

    @Override
    public void publish(EventEnvelope envelope) {
        log.info("No broker configured — event not published: type={} eventId={} tenantId={}",
                envelope.eventType(), envelope.eventId(), envelope.tenantId());
    }

    @Override
    public void publish(EventEnvelope envelope, String destination) {
        log.info("No broker configured — event not published: type={} eventId={} tenantId={} destination={}",
                envelope.eventType(), envelope.eventId(), envelope.tenantId(), destination);
    }
}
