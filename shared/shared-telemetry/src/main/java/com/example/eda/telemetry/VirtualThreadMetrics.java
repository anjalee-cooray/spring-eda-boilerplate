package com.example.eda.telemetry;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.springframework.lang.NonNull;

public class VirtualThreadMetrics implements MeterBinder {

    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    @Override
    public void bindTo(@NonNull MeterRegistry registry) {
        Gauge.builder("jvm.threads.total", threadMXBean, ThreadMXBean::getThreadCount)
                .description("Total number of live threads including virtual threads")
                .register(registry);

        Gauge.builder("jvm.threads.peak", threadMXBean, ThreadMXBean::getPeakThreadCount)
                .description("Peak live thread count since JVM start or reset")
                .register(registry);

        Gauge.builder("jvm.threads.daemon", threadMXBean, ThreadMXBean::getDaemonThreadCount)
                .description("Current number of daemon threads")
                .register(registry);
    }
}
