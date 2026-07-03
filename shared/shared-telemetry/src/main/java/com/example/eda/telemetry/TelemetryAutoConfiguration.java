package com.example.eda.telemetry;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration
public class TelemetryAutoConfiguration {

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> commonTags(Environment env) {
        String appName = env.getProperty("spring.application.name", "unknown");
        return registry -> registry.config()
                .commonTags("service", appName);
    }

    @Bean
    @ConditionalOnMissingBean
    public VirtualThreadMetrics virtualThreadMetrics() {
        return new VirtualThreadMetrics();
    }
}
