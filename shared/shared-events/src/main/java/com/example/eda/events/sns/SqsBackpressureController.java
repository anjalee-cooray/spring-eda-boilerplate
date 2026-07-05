package com.example.eda.events.sns;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Bang-bang backpressure controller for SQS consumers — SNS/SQS equivalent of
 * KafkaBackpressureController.
 *
 * Periodically reads the source queue depth from SqsConsumerLagMetrics.
 * When depth exceeds lagPauseThreshold, sets isPaused = true. SqsEventConsumer
 * checks this flag at the top of poll() and skips the SQS receive call while
 * paused — allowing the processing thread to drain in-flight messages before
 * fetching more.
 *
 * Polling resumes when queue depth drops below lagResumeThreshold.
 *
 * Unlike Kafka (where pause() stops the consumer thread from fetching), SQS
 * consumers are driven by a @Scheduled poller — backpressure is implemented by
 * returning early from poll(), which is simpler and equally effective.
 *
 * Tuning:
 *   app.consumer.backpressure.lag-pause-threshold   default 10000
 *   app.consumer.backpressure.lag-resume-threshold  default 1000
 *   app.consumer.backpressure.check-interval-ms     default 5000
 *
 * Only active when app.events.broker=sns.
 */
@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "sns")
public class SqsBackpressureController {

    private static final Logger log = LoggerFactory.getLogger(SqsBackpressureController.class);

    private final SqsConsumerLagMetrics lagMetrics;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    @Value("${app.consumer.backpressure.lag-pause-threshold:10000}")
    private long lagPauseThreshold;

    @Value("${app.consumer.backpressure.lag-resume-threshold:1000}")
    private long lagResumeThreshold;

    public SqsBackpressureController(SqsConsumerLagMetrics lagMetrics, MeterRegistry meterRegistry) {
        this.lagMetrics = lagMetrics;

        // Gauge: 1 = paused (backpressure active), 0 = running — mirrors Kafka gauge
        Gauge.builder("consumer.backpressure.active", paused, p -> p.get() ? 1.0 : 0.0)
                .description("1 when SQS consumer polling is paused due to high queue depth, 0 when running")
                .register(meterRegistry);
    }

    @PostConstruct
    public void logConfig() {
        log.info("SqsBackpressureController active: pauseThreshold={} resumeThreshold={}",
                lagPauseThreshold, lagResumeThreshold);
    }

    @Scheduled(fixedDelayString = "${app.consumer.backpressure.check-interval-ms:5000}")
    public void checkAndAdjust() {
        long depth = lagMetrics.getMaxDepth();

        if (!paused.get() && depth > lagPauseThreshold) {
            paused.set(true);
            log.warn("SQS polling PAUSED — queue depth={} exceeded threshold={}. "
                    + "Resume threshold={}. Monitor consumer.backpressure.active gauge.",
                    depth, lagPauseThreshold, lagResumeThreshold);
        } else if (paused.get() && depth < lagResumeThreshold) {
            paused.set(false);
            log.info("SQS polling RESUMED — queue depth={} dropped below resume threshold={}",
                    depth, lagResumeThreshold);
        } else if (paused.get()) {
            log.debug("SQS polling still paused — queue depth={} resume threshold={}",
                    depth, lagResumeThreshold);
        }
    }

    public boolean isPaused() {
        return paused.get();
    }
}
