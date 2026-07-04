package com.example.eda.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

/**
 * Auto-configuration for Resilience4j circuit breaker and retry patterns.
 *
 * Decorator order (outermost to innermost):
 *   CircuitBreaker → Retry → actual call
 *
 * The circuit breaker wraps the retry so it sees the final outcome after
 * all retry attempts are exhausted — not each individual attempt.
 * This prevents retries from tripping the circuit breaker prematurely.
 */
@AutoConfiguration
@ComponentScan(basePackages = "com.example.eda.resilience")
public class ResilienceAutoConfiguration {

    /**
     * Binds circuit breaker metrics to Micrometer.
     * Exposes: resilience4j_circuitbreaker_state, _failure_rate, _calls_total
     */
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    public TaggedCircuitBreakerMetrics circuitBreakerMetrics(
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry) {
        TaggedCircuitBreakerMetrics metrics =
                TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }

    /**
     * Binds retry metrics to Micrometer.
     * Exposes: resilience4j_retry_calls_total (with outcome: successful_with_retry,
     * failed_with_retry, successful_without_retry, failed_without_retry)
     */
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    public TaggedRetryMetrics retryMetrics(
            RetryRegistry retryRegistry,
            MeterRegistry meterRegistry) {
        TaggedRetryMetrics metrics = TaggedRetryMetrics.ofRetryRegistry(retryRegistry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }
}
