package com.example.eda.telemetry;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualThreadMetricsTest {

    @Test
    void registersThreadGauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new VirtualThreadMetrics().bindTo(registry);

        Gauge total = registry.find("jvm.threads.total").gauge();
        Gauge peak = registry.find("jvm.threads.peak").gauge();
        Gauge daemon = registry.find("jvm.threads.daemon").gauge();

        assertThat(total).isNotNull();
        assertThat(peak).isNotNull();
        assertThat(daemon).isNotNull();
    }

    @Test
    void threadCountIsPositive() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new VirtualThreadMetrics().bindTo(registry);

        double count = registry.find("jvm.threads.total").gauge().value();
        assertThat(count).isGreaterThan(0);
    }
}
