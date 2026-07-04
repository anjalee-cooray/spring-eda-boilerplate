package com.example.eda.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@ComponentScan(basePackages = "com.example.eda.resilience")
public class ResilienceAutoConfiguration {

    /**
     * Binds all Resilience4j circuit breaker metrics to Micrometer automatically.
     * Exposes: resilience4j_circuitbreaker_state, _failure_rate, _calls_total, etc.
     * These appear in /actuator/prometheus and are queryable in Grafana.
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
}
