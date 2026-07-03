package com.example.eda.events.kafka;

import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.publisher.EventPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "kafka")
public class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(EventEnvelope envelope) {
        publish(envelope, envelope.eventType());
    }

    @Override
    public void publish(EventEnvelope envelope, String destination) {
        String payload = serialize(envelope);
        kafkaTemplate.send(destination, envelope.tenantId(), payload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish event type={} eventId={} to topic={}",
                                envelope.eventType(), envelope.eventId(), destination, ex);
                    } else {
                        log.debug("Published event type={} eventId={} to topic={}",
                                envelope.eventType(), envelope.eventId(), destination);
                    }
                });
    }

    private String serialize(EventEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("Failed to serialize event envelope", e);
        }
    }
}
