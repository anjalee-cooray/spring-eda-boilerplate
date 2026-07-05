package com.example.eda.events.sns;

import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.kafka.EventPublishException;
import com.example.eda.events.publisher.EventPublisher;
import com.example.eda.events.schema.EventSchemaRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "sns")
public class SnsEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(SnsEventPublisher.class);

    private final SnsClient snsClient;
    private final ObjectMapper objectMapper;
    private final SnsProperties properties;
    private final Optional<EventSchemaRegistry> schemaRegistry;

    public SnsEventPublisher(
            SnsClient snsClient,
            ObjectMapper objectMapper,
            SnsProperties properties,
            Optional<EventSchemaRegistry> schemaRegistry) {
        this.snsClient = snsClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public void publish(EventEnvelope envelope) {
        String topicArn = properties.topicArn(envelope.eventType());
        publish(envelope, topicArn);
    }

    @Override
    public void publish(EventEnvelope envelope, String destination) {
        schemaRegistry.ifPresent(r -> r.validate(envelope));
        String payload = serialize(envelope);

        PublishRequest.Builder requestBuilder = PublishRequest.builder()
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
                            .build(),
                    "schema_version", MessageAttributeValue.builder()
                            .dataType("String")
                            .stringValue(envelope.schemaVersion())
                            .build()
                ));

        // FIFO topics (ARN ends with ".fifo") require MessageGroupId for per-entity ordering
        // and support MessageDeduplicationId to deduplicate retried publishes within a 5-minute window.
        //
        //   MessageGroupId       = tenantId:aggregateId  → all events for the same entity land in the
        //                          same FIFO group, guaranteeing delivery order (mirrors Kafka partition key).
        //                          Falls back to tenantId when aggregateId is not set.
        //
        //   MessageDeduplicationId = eventId             → SNS deduplicates within a 5-minute window.
        //                          If the relay publishes the same event_id twice (crash between Phase 2
        //                          and Phase 3), SNS silently drops the second copy at the broker.
        //
        // Standard topics do not support these fields — passing them causes an error.
        if (isFifoTopic(destination)) {
            requestBuilder
                    .messageGroupId(partitionKey(envelope))
                    .messageDeduplicationId(envelope.eventId().toString());
        }

        PublishRequest request = requestBuilder.build();

        try {
            snsClient.publish(request);
            log.debug("Published event type={} eventId={} schemaVersion={} fifo={} to topicArn={}",
                    envelope.eventType(), envelope.eventId(), envelope.schemaVersion(),
                    isFifoTopic(destination), destination);
        } catch (Exception ex) {
            log.error("Failed to publish event type={} eventId={} to topicArn={}",
                    envelope.eventType(), envelope.eventId(), destination, ex);
            throw ex;
        }
    }

    /**
     * Returns the SNS/SQS FIFO partition key — mirrors KafkaEventPublisher.partitionKey().
     * All events for the same entity share a MessageGroupId → FIFO delivery order guaranteed.
     */
    private String partitionKey(EventEnvelope envelope) {
        if (envelope.aggregateId() != null && !envelope.aggregateId().isBlank()) {
            return envelope.tenantId() + ":" + envelope.aggregateId();
        }
        return envelope.tenantId();
    }

    /** SNS FIFO topic ARNs always end with ".fifo". Standard topics do not. */
    private boolean isFifoTopic(String topicArn) {
        return topicArn != null && topicArn.endsWith(".fifo");
    }

    private String serialize(EventEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("Failed to serialize event envelope", e);
        }
    }
}
