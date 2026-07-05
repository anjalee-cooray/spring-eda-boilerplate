package com.example.eda.events.sns;

import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

/**
 * Polls SQS for domain events and dispatches to registered EventConsumer handlers.
 *
 * Retry and DLQ strategy (SQS/SNS path):
 *
 *   On success  → delete the message from SQS (prevents re-delivery)
 *   On failure  → do NOT delete; extend visibility timeout with exponential backoff
 *                 so the message is not re-delivered immediately:
 *                   receiveCount=1 → 10s
 *                   receiveCount=2 → 30s
 *                   receiveCount=3 → 90s
 *
 * After maxReceiveCount deliveries (configured on the SQS queue Redrive Policy),
 * AWS automatically moves the message to the DLQ. No application code needed for routing.
 *
 * Contrast with Kafka:
 *   Kafka: application retries in-process, then routes to DLQ topic via DeadLetterPublishingRecoverer
 *   SQS:   AWS retries via re-delivery, routes to DLQ via Redrive Policy — infra-level, not app-level
 */
@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "sns")
public class SqsEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SqsEventConsumer.class);

    // Visibility timeout seconds per receive count (exponential backoff)
    private static final int[] BACKOFF_SECONDS = {10, 30, 90};

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final List<EventConsumer> consumers;
    private final SqsProperties sqsProperties;

    public SqsEventConsumer(
            SqsClient sqsClient,
            ObjectMapper objectMapper,
            List<EventConsumer> consumers,
            SqsProperties sqsProperties) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.consumers = consumers;
        this.sqsProperties = sqsProperties;
    }

    @Scheduled(fixedDelayString = "${app.events.sqs.poll-interval-ms:1000}")
    public void poll() {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(sqsProperties.queueUrl())
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                // Required to read ApproximateReceiveCount for backoff calculation
                .attributeNamesWithStrings(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString())
                .build();

        List<Message> messages = sqsClient.receiveMessage(request).messages();

        for (Message message : messages) {
            process(message);
        }
    }

    private void process(Message message) {
        try {
            EventEnvelope envelope = objectMapper.readValue(message.body(), EventEnvelope.class);
            consumers.stream()
                    .filter(c -> c.supports(envelope.eventType()))
                    .forEach(c -> c.handle(envelope));

            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(sqsProperties.queueUrl())
                    .receiptHandle(message.receiptHandle())
                    .build());

        } catch (Exception ex) {
            int receiveCount = parseReceiveCount(message);
            int backoffSecs = backoffSeconds(receiveCount);

            log.error("Failed to process SQS message messageId={} receiveCount={} backoffSeconds={} — extending visibility timeout",
                    message.messageId(), receiveCount, backoffSecs, ex);

            // Extend visibility timeout so the message is not re-delivered immediately.
            // After maxReceiveCount deliveries, AWS routes it to the DLQ automatically.
            extendVisibility(message, backoffSecs);
        }
    }

    private void extendVisibility(Message message, int seconds) {
        try {
            sqsClient.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                    .queueUrl(sqsProperties.queueUrl())
                    .receiptHandle(message.receiptHandle())
                    .visibilityTimeout(seconds)
                    .build());
        } catch (Exception ex) {
            log.warn("Failed to extend visibility timeout for messageId={} — SQS will re-deliver after original timeout",
                    message.messageId(), ex);
        }
    }

    private int parseReceiveCount(Message message) {
        try {
            String raw = message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
            return raw != null ? Integer.parseInt(raw) : 1;
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    private int backoffSeconds(int receiveCount) {
        int index = Math.min(receiveCount - 1, BACKOFF_SECONDS.length - 1);
        return BACKOFF_SECONDS[Math.max(0, index)];
    }
}
