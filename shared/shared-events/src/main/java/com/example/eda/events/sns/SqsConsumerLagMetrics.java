package com.example.eda.events.sns;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * Exposes SQS queue depth as Micrometer gauges — the SNS/SQS equivalent of
 * KafkaConsumerLagMetrics.
 *
 * sqs.consumer.lag{queue, type} reports the number of messages currently in the
 * queue (visible + in-flight). A growing value means the consumer is processing
 * slower than messages arrive.
 *
 *   type=source → main event queue (events not yet processed)
 *   type=dlq    → dead letter queue (events that exhausted all retries)
 *
 * Values are read from SQS GetQueueAttributes on a schedule and cached in a
 * snapshot — Prometheus scrapes are never blocked by a network call.
 *
 * CloudWatch alternative:
 *   AWS/SQS ApproximateNumberOfMessagesVisible on each queue provides the same
 *   signal and can trigger a CloudWatch alarm independently of this service.
 *   Both should be configured — CloudWatch fires even when the service is down.
 *
 * Only active when app.events.broker=sns.
 */
@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "sns")
public class SqsConsumerLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(SqsConsumerLagMetrics.class);

    private final SqsClient sqsClient;
    private final SqsProperties sqsProperties;
    private final MeterRegistry meterRegistry;

    // queueUrl → approximate message count (visible + in-flight)
    private final Map<String, Long> depthSnapshot = new ConcurrentHashMap<>();

    @Value("${app.sqs.lag-refresh-ms:15000}")
    private long lagRefreshMs;

    @Value("${app.consumer.backpressure.lag-warn-threshold:5000}")
    private long lagWarnThreshold;

    public SqsConsumerLagMetrics(
            SqsClient sqsClient,
            SqsProperties sqsProperties,
            MeterRegistry meterRegistry) {
        this.sqsClient = sqsClient;
        this.sqsProperties = sqsProperties;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void init() {
        log.info("SqsConsumerLagMetrics initialised sourceQueue={} dlqQueue={}",
                sqsProperties.queueUrl(), sqsProperties.dlqQueueUrl());
    }

    @Scheduled(fixedDelayString = "${app.sqs.lag-refresh-ms:15000}")
    public void refreshDepth() {
        refreshQueue(sqsProperties.queueUrl(), "source");

        if (sqsProperties.dlqQueueUrl() != null && !sqsProperties.dlqQueueUrl().isBlank()) {
            refreshQueue(sqsProperties.dlqQueueUrl(), "dlq");
        }
    }

    private void refreshQueue(String queueUrl, String type) {
        try {
            var response = sqsClient.getQueueAttributes(
                    GetQueueAttributesRequest.builder()
                            .queueUrl(queueUrl)
                            .attributeNames(
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                            .build());

            long visible  = parseLong(response.attributes().get(
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
            long inFlight = parseLong(response.attributes().get(
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
            long total = visible + inFlight;

            depthSnapshot.put(queueUrl, total);

            // Lazily register gauge on first observation of this queue
            Gauge.builder("sqs.consumer.lag", depthSnapshot,
                            m -> m.getOrDefault(queueUrl, 0L).doubleValue())
                    .tags("queue", queueName(queueUrl), "type", type)
                    .description("SQS queue depth (visible + in-flight messages)")
                    .register(meterRegistry);

            if (total > lagWarnThreshold) {
                log.warn("SQS queue depth high queue={} type={} visible={} inFlight={} total={} threshold={}",
                        queueName(queueUrl), type, visible, inFlight, total, lagWarnThreshold);
            } else {
                log.debug("SQS queue depth queue={} type={} visible={} inFlight={} total={}",
                        queueName(queueUrl), type, visible, inFlight, total);
            }

        } catch (Exception ex) {
            log.warn("Failed to refresh SQS queue depth for queue={} type={}: {}",
                    queueName(queueUrl), type, ex.getMessage());
        }
    }

    /** Returns the maximum queue depth across all tracked queues — 0 if no data yet. */
    public long getMaxDepth() {
        // DLQ depth is excluded from the backpressure signal — DLQ messages are already
        // dead, not pending processing. Only the source queue drives backpressure.
        String sourceUrl = sqsProperties.queueUrl();
        return depthSnapshot.getOrDefault(sourceUrl, 0L);
    }

    /** Returns the DLQ depth — non-zero means failed events need operator attention. */
    public long getDlqDepth() {
        String dlqUrl = sqsProperties.dlqQueueUrl();
        return dlqUrl != null ? depthSnapshot.getOrDefault(dlqUrl, 0L) : 0L;
    }

    private long parseLong(String value) {
        if (value == null) return 0L;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private String queueName(String queueUrl) {
        if (queueUrl == null) return "unknown";
        int lastSlash = queueUrl.lastIndexOf('/');
        return lastSlash >= 0 ? queueUrl.substring(lastSlash + 1) : queueUrl;
    }
}
