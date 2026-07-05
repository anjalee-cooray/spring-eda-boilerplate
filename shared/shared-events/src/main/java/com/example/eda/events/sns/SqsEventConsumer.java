package com.example.eda.events.sns;

import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.schema.EventUpcasterRegistry;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Polls SQS for domain events and dispatches to registered EventConsumer handlers.
 *
 * Retry and DLQ strategy (SQS/SNS path):
 *
 *   On success        → delete the message from SQS (prevents re-delivery)
 *   On retryable fail → do NOT delete; extend visibility timeout with exponential backoff
 *                       so the message is not re-delivered immediately:
 *                         receiveCount=1 → 10s
 *                         receiveCount=2 → 30s
 *                         receiveCount=3 → 90s
 *                       After maxReceiveCount deliveries (SQS Redrive Policy),
 *                       AWS moves the message to the DLQ automatically.
 *   On non-retryable  → delete from source queue and write directly to DLQ.
 *                       Skips all retry attempts — a malformed payload will never
 *                       succeed, so wasting maxReceiveCount retries is avoided.
 *                       Mirrors KafkaConsumerConfig.addNotRetryableExceptions().
 *
 * Non-retryable exception types:
 *   IllegalArgumentException   — business validation or unsupported event type
 *   IllegalStateException      — state machine invariant violated
 *   JsonParseException         — payload is not valid JSON and will never be
 *
 * Contrast with Kafka:
 *   Kafka: application retries in-process, then routes to DLQ topic via DeadLetterPublishingRecoverer;
 *          non-retryable exceptions bypass retries via addNotRetryableExceptions()
 *   SQS:   AWS retries via re-delivery (Redrive Policy); non-retryable exceptions are
 *          now handled by this class — direct DLQ write + source delete
 */
@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "sns")
public class SqsEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SqsEventConsumer.class);

    // Visibility timeout seconds per receive count (exponential backoff)
    private static final int[] BACKOFF_SECONDS = {10, 30, 90};

    // Exceptions that will never succeed on retry — route directly to DLQ without
    // consuming maxReceiveCount attempts. Add new types here as handlers mature.
    private static final Set<Class<? extends Throwable>> NON_RETRYABLE = Set.of(
            IllegalArgumentException.class,
            IllegalStateException.class,
            JsonParseException.class
    );

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final List<EventConsumer> consumers;
    private final SqsProperties sqsProperties;
    private final Optional<EventUpcasterRegistry> upcasterRegistry;
    private final Optional<SqsBackpressureController> backpressureController;

    public SqsEventConsumer(
            SqsClient sqsClient,
            ObjectMapper objectMapper,
            List<EventConsumer> consumers,
            SqsProperties sqsProperties,
            Optional<EventUpcasterRegistry> upcasterRegistry,
            Optional<SqsBackpressureController> backpressureController) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.consumers = consumers;
        this.sqsProperties = sqsProperties;
        this.upcasterRegistry = upcasterRegistry;
        this.backpressureController = backpressureController;
    }

    @Scheduled(fixedDelayString = "${app.events.sqs.poll-interval-ms:1000}")
    public void poll() {
        // Skip this poll cycle when backpressure is active.
        // SqsBackpressureController sets isPaused when the source queue depth exceeds
        // lag-pause-threshold, giving the processing thread time to drain in-flight
        // messages before fetching more. Polling resumes automatically.
        if (backpressureController.map(SqsBackpressureController::isPaused).orElse(false)) {
            log.debug("SQS poll skipped — backpressure active");
            return;
        }

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
            EventEnvelope raw = objectMapper.readValue(message.body(), EventEnvelope.class);
            EventEnvelope envelope = upcasterRegistry
                    .map(r -> r.upcastToLatest(raw))
                    .orElse(raw);
            consumers.stream()
                    .filter(c -> c.supports(envelope.eventType()))
                    .forEach(c -> c.handle(envelope));

            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(sqsProperties.queueUrl())
                    .receiptHandle(message.receiptHandle())
                    .build());

        } catch (Exception ex) {
            if (isNonRetryable(ex)) {
                // Non-retryable: write to DLQ immediately, delete from source.
                // Retrying this message will never succeed — don't waste maxReceiveCount attempts.
                routeToDlq(message, ex);
                deleteFromSource(message);
            } else {
                int receiveCount = parseReceiveCount(message);
                int backoffSecs = backoffSeconds(receiveCount);
                log.error("Failed to process SQS message messageId={} receiveCount={} backoffSeconds={} — extending visibility timeout",
                        message.messageId(), receiveCount, backoffSecs, ex);
                // Extend visibility timeout; AWS routes to DLQ after maxReceiveCount via Redrive Policy.
                extendVisibility(message, backoffSecs);
            }
        }
    }

    private boolean isNonRetryable(Exception ex) {
        Throwable cause = ex;
        while (cause != null) {
            for (Class<? extends Throwable> type : NON_RETRYABLE) {
                if (type.isInstance(cause)) return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private void routeToDlq(Message message, Exception ex) {
        String dlqUrl = sqsProperties.dlqQueueUrl();
        if (dlqUrl == null || dlqUrl.isBlank()) {
            log.error("Non-retryable SQS message and no DLQ configured — message will be deleted. "
                    + "messageId={} error={}", message.messageId(), ex.getMessage(), ex);
            return;
        }

        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(dlqUrl)
                    .messageBody(message.body())
                    .build());
            log.error("Non-retryable SQS message routed to DLQ — operator action required. "
                    + "messageId={} dlqUrl={} errorClass={} error={}",
                    message.messageId(), dlqUrl, ex.getClass().getSimpleName(), ex.getMessage(), ex);
        } catch (Exception dlqEx) {
            log.error("Failed to route non-retryable message to DLQ — message will be deleted from source. "
                    + "messageId={}", message.messageId(), dlqEx);
        }
    }

    private void deleteFromSource(Message message) {
        try {
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(sqsProperties.queueUrl())
                    .receiptHandle(message.receiptHandle())
                    .build());
        } catch (Exception ex) {
            log.warn("Failed to delete non-retryable message from source queue — "
                    + "it will be re-delivered after visibility timeout and routed to DLQ again. "
                    + "messageId={}", message.messageId(), ex);
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
