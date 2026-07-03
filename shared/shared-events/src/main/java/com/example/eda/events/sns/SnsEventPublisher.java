package com.example.eda.events.sns;

import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.kafka.EventPublishException;
import com.example.eda.events.publisher.EventPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.util.Map;

@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "sns")
public class SnsEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(SnsEventPublisher.class);

    private final SnsClient snsClient;
    private final ObjectMapper objectMapper;
    private final SnsProperties properties;

    public SnsEventPublisher(SnsClient snsClient, ObjectMapper objectMapper, SnsProperties properties) {
        this.snsClient = snsClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Override
    public void publish(EventEnvelope envelope) {
        String topicArn = properties.topicArn(envelope.eventType());
        publish(envelope, topicArn);
    }

    @Override
    public void publish(EventEnvelope envelope, String destination) {
        String payload = serialize(envelope);

        PublishRequest request = PublishRequest.builder()
                .topicArn(destination)
                .message(payload)
                .messageAttributes(Map.of(
                    "event_type", MessageAttributeValue.builder()
                            .dataType("String")
                            .stringValue(envelope.eventType())
                            .build(),
                    "tenant_id", MessageAttributeValue.builder()
                            .dataType("String")
                            .stringValue(envelope.tenantId())
                            .build()
                ))
                .build();

        try {
            snsClient.publish(request);
            log.debug("Published event type={} eventId={} to topicArn={}",
                    envelope.eventType(), envelope.eventId(), destination);
        } catch (Exception ex) {
            log.error("Failed to publish event type={} eventId={} to topicArn={}",
                    envelope.eventType(), envelope.eventId(), destination, ex);
            throw ex;
        }
    }

    private String serialize(EventEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("Failed to serialize event envelope", e);
        }
    }
}
