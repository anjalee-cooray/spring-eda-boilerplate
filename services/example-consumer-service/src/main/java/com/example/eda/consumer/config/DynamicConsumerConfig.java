package com.example.eda.consumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

/**
 * Runtime-refreshable configuration for the consumer service.
 *
 * KafkaBackpressureController and SqsBackpressureController use @Value fields
 * injected at construction time. Because they are @Scheduled, @RefreshScope
 * cannot update their @Value fields at runtime (the scheduler holds a direct
 * reference, bypassing the refresh proxy).
 *
 * Pattern for truly dynamic thresholds in scheduled beans:
 *   Inject DynamicConsumerConfig into the controller and call
 *   getLagPauseThreshold() in checkAndAdjust() instead of using a @Value field.
 *   The DynamicConsumerConfig proxy is refreshed, so the next tick reads the
 *   new threshold automatically.
 *
 * This bean demonstrates the pattern. Wiring it into backpressure controllers
 * is left as a follow-up so the controller diff stays minimal.
 */
@Component
@RefreshScope
public class DynamicConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(DynamicConsumerConfig.class);

    @Value("${app.consumer.backpressure.lag-pause-threshold:10000}")
    private long lagPauseThreshold;

    @Value("${app.consumer.backpressure.lag-resume-threshold:1000}")
    private long lagResumeThreshold;

    @Value("${app.consumer.backpressure.lag-warn-threshold:5000}")
    private long lagWarnThreshold;

    @Value("${app.features.saga-enabled:true}")
    private boolean sagaEnabled;

    @Value("${app.features.dlq-consumer-enabled:true}")
    private boolean dlqConsumerEnabled;

    public long getLagPauseThreshold() { return lagPauseThreshold; }
    public long getLagResumeThreshold() { return lagResumeThreshold; }
    public long getLagWarnThreshold() { return lagWarnThreshold; }
    public boolean isSagaEnabled() { return sagaEnabled; }
    public boolean isDlqConsumerEnabled() { return dlqConsumerEnabled; }

    @jakarta.annotation.PostConstruct
    public void logConfig() {
        log.info("DynamicConsumerConfig loaded: lagPause={} lagResume={} sagaEnabled={} dlqEnabled={}",
                lagPauseThreshold, lagResumeThreshold, sagaEnabled, dlqConsumerEnabled);
    }
}
