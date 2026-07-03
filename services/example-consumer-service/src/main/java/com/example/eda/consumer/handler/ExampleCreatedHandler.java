package com.example.eda.consumer.handler;

import com.example.eda.db.inbox.InboxDeduplicator;
import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class ExampleCreatedHandler implements EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ExampleCreatedHandler.class);

    private final InboxDeduplicator deduplicator;

    public ExampleCreatedHandler(InboxDeduplicator deduplicator) {
        this.deduplicator = deduplicator;
    }

    @Override
    public boolean supports(String eventType) {
        return "example.created".equals(eventType);
    }

    @Override
    @Transactional
    public void handle(EventEnvelope envelope) {
        if (deduplicator.isDuplicate(envelope.eventId())) {
            log.info("Skipping duplicate event eventId={} eventType={}",
                    envelope.eventId(), envelope.eventType());
            return;
        }

        log.info("Handling example.created eventId={} tenantId={}",
                envelope.eventId(), envelope.tenantId());

        // TODO: implement business reaction to example.created
        // e.g. send a welcome notification, trigger a downstream process, etc.

        deduplicator.markProcessed(
                envelope.eventId(),
                envelope.eventType(),
                envelope.tenantId()
        );
    }
}
