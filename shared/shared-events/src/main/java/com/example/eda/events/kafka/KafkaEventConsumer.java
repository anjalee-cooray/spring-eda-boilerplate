package com.example.eda.events.kafka;

import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "kafka")
public class KafkaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);

    private final List<EventConsumer> consumers;
    private final ObjectMapper objectMapper;

    public KafkaEventConsumer(List<EventConsumer> consumers, ObjectMapper objectMapper) {
        this.consumers = consumers;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.events.kafka.topics}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = deserialize(record.value());
        if (envelope == null) {
            return;
        }

        consumers.stream()
                .filter(c -> c.supports(envelope.eventType()))
                .forEach(c -> {
                    try {
                        c.handle(envelope);
                    } catch (Exception ex) {
                        log.error("Consumer {} failed to handle event type={} eventId={}",
                                c.getClass().getSimpleName(), envelope.eventType(), envelope.eventId(), ex);
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
