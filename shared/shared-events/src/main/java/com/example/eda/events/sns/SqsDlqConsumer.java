package com.example.eda.events.sns;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Polls the SQS DLQ and emits observability signals for operator triage.
 *
 * AWS automatically moves messages to the DLQ after maxReceiveCount deliveries
 * on the source queue (Redrive Policy). This consumer reads from the DLQ,
 * logs the full message for operator inspection, increments the Micrometer
 * counter, then deletes the message.
 *
 * Why delete from DLQ:
 *   The DLQ message is deleted after logging so it is not re-emitted on the
 *   next poll. The operator uses the logs (and the Prometheus alert that fired)
 *   to understand what failed. To replay a fixed handler against the original
 *   source, use the AWS Console or CLI to move the messages from DLQ back to
 *   the source queue (SQS "Start DLQ redrive" feature).
 *
 * Prometheus alert to configure:
 *   rate(events_dlq_received_total[5m]) > 0
 *   severity: P1
 *
 * CloudWatch alarm (alternative / supplement):
 *   Metric: AWS/SQS ApproximateNumberOfMessagesVisible on the DLQ queue
 *   Threshold: >= 1
 *   This fires before this consumer processes the DLQ message, which is useful
 *   when the consumer service itself is down.
 *
 * Operator runbook:
 *   1. Find the DLQ log line — it contains messageId, source queue, receive count, and body
 *   2. Identify the root cause from the body (check the original handler logs for the error)
 *   3. Code bug  → fix the handler, redeploy, then replay using AWS Console:
 *        SQS → select DLQ → Start DLQ redrive → redrive to source queue
 *   4. Bad data  → message is already deleted from DLQ after logging; no further action
 *
 * Only active when dlq-queue-url is configured. If unset, the bean is skipped.
 */
@Component
@ConditionalOnProperty(name = "app.events.sqs.dlq-queue-url")
public class SqsDlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(SqsDlqConsumer.class);

    private final SqsClient sqsClient;
    private final SqsProperties sqsProperties;
    private final MeterRegistry meterRegistry;
    private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();

    public SqsDlqConsumer(
            SqsClient sqsClient,
            SqsProperties sqsProperties,
            MeterRegistry meterRegistry) {
        this.sqsClient = sqsClient;
        this.sqsProperties = sqsProperties;
        this.meterRegistry = meterRegistry;
    }

    @Scheduled(fixedDelayString = "${app.events.sqs.dlq-poll-interval-ms:5000}")
    public void poll() {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(sqsProperties.dlqQueueUrl())
                .maxNumberOfMessages(10)
                .waitTimeSeconds(5)
                .attributeNamesWithStrings(
                        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString(),
                        MessageSystemAttributeName.SENDER_ID.toString()
                )
                .build();

        List<Message> messages = sqsClient.receiveMessage(request).messages();

        for (Message message : messages) {
            handle(message);
        }
    }

    private void handle(Message message) {
        String queueUrl = sqsProperties.dlqQueueUrl();
        String queueName = queueName(queueUrl);
        String receiveCount = message.attributes()
                .getOrDefault(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "unknown");

        counterFor(queueName).increment();

        // Log at ERROR — every DLQ message means a business event was not processed
        log.error("DLQ message received — operator action required "
                        + "dlqQueue={} messageId={} receiveCount={} body={}",
                queueName, message.messageId(), receiveCount, message.body());

        // Delete after logging so it is not re-emitted on the next poll cycle.
        // Replay is done via AWS Console "Start DLQ redrive" — see runbook above.
        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build());
    }

    private Counter counterFor(String queueName) {
        return counters.computeIfAbsent(queueName, name ->
                Counter.builder("events.dlq.received")
                        .tag("queue", name)
                        .description("Number of events routed to the SQS dead letter queue")
                        .register(meterRegistry));
    }

    private String queueName(String queueUrl) {
        // Extract queue name from URL: https://sqs.us-east-1.amazonaws.com/123/example-dlq
        int lastSlash = queueUrl.lastIndexOf('/');
        return lastSlash >= 0 ? queueUrl.substring(lastSlash + 1) : queueUrl;
    }
}
