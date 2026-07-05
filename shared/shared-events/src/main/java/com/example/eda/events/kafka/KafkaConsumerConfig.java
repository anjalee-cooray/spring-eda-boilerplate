package com.example.eda.events.kafka;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Kafka consumer error handling — retry with exponential backoff, then DLQ.
 *
 * Retry policy: 3 attempts with exponential backoff (1s → 2s → 4s).
 * After all retries are exhausted, the message is sent to {original-topic}.dlq.
 *
 * DLQ topic naming convention: {original-topic}.dlq
 *   example.created  →  example.created.dlq
 *
 * The DLQ message carries Spring Kafka DLT headers:
 *   kafka_dlt-original-topic     — source topic
 *   kafka_dlt-original-partition — source partition
 *   kafka_dlt-original-offset    — source offset
 *   kafka_dlt-exception-message  — exception message
 *   kafka_dlt-exception-fqcn     — exception class
 *
 * IllegalArgumentException and similar non-retryable errors skip retries
 * and go directly to DLQ — retrying a malformed payload will never succeed.
 *
 * Operator actions when DLQ depth > 0:
 *   1. Read the DLQ consumer logs — kafka_dlt-original-* headers give the source location
 *   2. Inspect the payload and exception to determine if it is a code bug or bad data
 *   3. Code bug  → deploy the fix, then replay from DLQ using kafka-consumer-groups --reset-offsets
 *   4. Bad data  → discard by consuming past the message (advance the DLQ consumer offset)
 */
@Configuration
@ConditionalOnProperty(name = "app.events.broker", havingValue = "kafka")
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private static final long INITIAL_INTERVAL_MS = 1_000L;
    private static final long MAX_INTERVAL_MS     = 8_000L;
    private static final double MULTIPLIER        = 2.0;
    private static final long MAX_ELAPSED_MS      = 30_000L; // 3 attempts: 1s + 2s + 4s

    @Bean
    public DefaultErrorHandler deadLetterErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                // Route to {original-topic}.dlq, partition -1 lets Kafka choose
                (record, ex) -> {
                    log.error("Routing to DLQ after retries exhausted topic={} partition={} offset={} exceptionClass={}",
                            record.topic(), record.partition(), record.offset(),
                            ex.getClass().getSimpleName(), ex);
                    return new TopicPartition(record.topic() + ".dlq", -1);
                }
        );

        ExponentialBackOff backOff = new ExponentialBackOff(INITIAL_INTERVAL_MS, MULTIPLIER);
        backOff.setMaxInterval(MAX_INTERVAL_MS);
        backOff.setMaxElapsedTime(MAX_ELAPSED_MS);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Skip retries for errors that will never succeed on retry.
        // A malformed payload or unsupported event type will fail every time.
        handler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                IllegalStateException.class
        );

        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler deadLetterErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(deadLetterErrorHandler);
        return factory;
    }
}
