package com.example.eda.consumer.dlq;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Consumes messages that failed all retry attempts and were routed to the DLQ.
 *
 * Responsibilities:
 *   1. Log the full failure context (original topic, offset, exception, payload)
 *   2. Increment events.dlq.received counter (tagged by topic) — drives the Prometheus alert
 *
 * This consumer deliberately does NOT re-throw. Re-throwing would cause the DLQ
 * message to be dead-lettered again, creating an infinite loop. Operator action
 * is required to resolve DLQ events — see the operator runbook below.
 *
 * Prometheus alert to configure:
 *   rate(events_dlq_received_total[5m]) > 0
 *   severity: P1 — every DLQ message means a business event was not processed.
 *
 * Operator runbook:
 *   1. Check logs for kafka_dlt-original-* headers — they identify the exact source message.
 *   2. Check the exception message to determine root cause.
 *   3. Code bug  → fix the handler, redeploy, then replay:
 *        kafka-consumer-groups --bootstrap-server localhost:9092 \
 *          --group example-consumer-service-dlq \
 *          --topic example.created.dlq \
 *          --reset-offsets --to-earliest --execute
 *   4. Bad data  → advance past the poison message:
 *        kafka-consumer-groups --bootstrap-server localhost:9092 \
 *          --group example-consumer-service-dlq \
 *          --topic example.created.dlq \
 *          --reset-offsets --to-latest --execute
 */
@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "kafka")
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    private final MeterRegistry meterRegistry;
    // Cache counters per topic so we don't create a new one on every message
    private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();

    public DlqConsumer(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(
            topics = "${app.events.kafka.dlq-topics}",
            groupId = "${spring.application.name}-dlq"
    )
    public void onDeadLetter(
            ConsumerRecord<String, String> record,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false) String exceptionClass,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) byte[] originalTopic,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_PARTITION, required = false) byte[] originalPartition,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) byte[] originalOffset) {

        String dlqTopic = record.topic();
        String sourceTopic = originalTopic != null ? new String(originalTopic) : "unknown";
        String sourcePartition = originalPartition != null ? new String(originalPartition) : "unknown";
        String sourceOffset = originalOffset != null ? new String(originalOffset) : "unknown";

        counterFor(dlqTopic).increment();

        // Log at ERROR so this surfaces in alerting dashboards even without Prometheus
        log.error("DLQ event received — operator action required "
                        + "dlqTopic={} sourceTopic={} sourcePartition={} sourceOffset={} "
                        + "exceptionClass={} exceptionMessage={} payload={}",
                dlqTopic, sourceTopic, sourcePartition, sourceOffset,
                exceptionClass, exceptionMessage, record.value());
    }

    private Counter counterFor(String topic) {
        return counters.computeIfAbsent(topic, t ->
                Counter.builder("events.dlq.received")
                        .tag("topic", t)
                        .description("Number of events routed to the dead letter queue")
                        .register(meterRegistry));
    }
}
