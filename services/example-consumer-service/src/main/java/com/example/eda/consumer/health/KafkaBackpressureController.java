package com.example.eda.consumer.health;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Bang-bang backpressure controller for Kafka consumers.
 *
 * Periodically reads the maximum partition lag from KafkaConsumerLagMetrics.
 * When lag exceeds lagPauseThreshold, all KafkaListenerContainers are paused —
 * the consumer stops fetching from the broker and the processing thread pool
 * drains the in-flight batch. Polling resumes when lag drops below lagResumeThreshold.
 *
 * This is intentionally simple: it pauses all containers together. For per-topic
 * granularity, call container.pause() only on containers whose topic lag is high
 * (identify by container.getAssignedPartitions()).
 *
 * Tuning:
 *   app.consumer.backpressure.lag-pause-threshold   default 10000
 *   app.consumer.backpressure.lag-resume-threshold  default 1000
 *   app.consumer.backpressure.check-interval-ms     default 5000
 *
 * If persistent lag cannot be resolved by pausing (the producer rate always
 * exceeds consumer rate), tune max.poll.records, fetch.max.bytes, and the
 * application's handle() execution time before lowering thresholds.
 *
 * Only active when app.events.broker=kafka. For SNS/SQS, use SqsBackpressureController.
 */
@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "kafka")
public class KafkaBackpressureController {

    private static final Logger log = LoggerFactory.getLogger(KafkaBackpressureController.class);

    private final KafkaListenerEndpointRegistry registry;
    private final KafkaConsumerLagMetrics lagMetrics;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    @Value("${app.consumer.backpressure.lag-pause-threshold:10000}")
    private long lagPauseThreshold;

    @Value("${app.consumer.backpressure.lag-resume-threshold:1000}")
    private long lagResumeThreshold;

    public KafkaBackpressureController(
            KafkaListenerEndpointRegistry registry,
            KafkaConsumerLagMetrics lagMetrics,
            MeterRegistry meterRegistry) {
        this.registry = registry;
        this.lagMetrics = lagMetrics;

        // Gauge: 1 = paused (backpressure active), 0 = running
        Gauge.builder("consumer.backpressure.active", paused, p -> p.get() ? 1.0 : 0.0)
                .description("1 when Kafka consumer is paused due to high lag, 0 when running")
                .register(meterRegistry);
    }

    @PostConstruct
    public void logConfig() {
        log.info("KafkaBackpressureController active: pauseThreshold={} resumeThreshold={}",
                lagPauseThreshold, lagResumeThreshold);
    }

    @Scheduled(fixedDelayString = "${app.consumer.backpressure.check-interval-ms:5000}")
    public void checkAndAdjust() {
        long maxLag = lagMetrics.getMaxLag();

        if (!paused.get() && maxLag > lagPauseThreshold) {
            registry.getListenerContainers().forEach(c -> c.pause());
            paused.set(true);
            log.warn("Kafka consumers PAUSED — lag={} exceeded threshold={}. "
                    + "Resume threshold={}. Monitor consumer.backpressure.active gauge.",
                    maxLag, lagPauseThreshold, lagResumeThreshold);
        } else if (paused.get() && maxLag < lagResumeThreshold) {
            registry.getListenerContainers().forEach(c -> c.resume());
            paused.set(false);
            log.info("Kafka consumers RESUMED — lag={} dropped below resume threshold={}",
                    maxLag, lagResumeThreshold);
        } else if (paused.get()) {
            log.debug("Kafka consumers still paused — lag={} resume threshold={}",
                    maxLag, lagResumeThreshold);
        }
    }
}
