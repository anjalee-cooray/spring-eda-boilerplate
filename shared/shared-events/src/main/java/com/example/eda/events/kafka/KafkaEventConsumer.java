package com.example.eda.events.kafka;

import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.schema.EventUpcasterRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "kafka")
public class KafkaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);

    private final List<EventConsumer> consumers;
    private final ObjectMapper objectMapper;
    private final Optional<EventUpcasterRegistry> upcasterRegistry;

    public KafkaEventConsumer(
            List<EventConsumer> consumers,
            ObjectMapper objectMapper,
            Optional<EventUpcasterRegistry> upcasterRegistry) {
        this.consumers = consumers;
        this.objectMapper = objectMapper;
        this.upcasterRegistry = upcasterRegistry;
    }

    @KafkaListener(topics = "${app.events.kafka.topics}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = deserialize(record.value());
        if (envelope == null) {
            return;
        }

        // Upcast to the latest schema version before dispatching to handlers.
        // No-op if no upcasters are registered or the event is already current.
        EventEnvelope current = upcasterRegistry
                .map(r -> r.upcastToLatest(envelope))
                .orElse(envelope);

        consumers.stream()
                .filter(c -> c.supports(current.eventType()))
                .forEach(c -> {
                    try {
                        c.handle(current);
                    } catch (Exception ex) {
                        log.error("Consumer {} failed to handle event type={} eventId={}",
                                c.getClass().getSimpleName(), current.eventType(), current.eventId(), ex);
                        throw ex;
                    }
                });
    }

    private EventEnvelope deserialize(String value) {
        try {
            return objectMapper.readValue(value, EventEnvelope.class);
        } catch (Exception e) {
            log.error("Failed to deserialize event envelope: {}", value, e);
            return null;
        }
    }
}
