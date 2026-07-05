package com.example.eda.events;

import com.example.eda.events.sns.SqsBackpressureController;
import com.example.eda.events.sns.SqsConsumerLagMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsBackpressureControllerTest {

    private final SqsConsumerLagMetrics lagMetrics = mock(SqsConsumerLagMetrics.class);
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private SqsBackpressureController controller;

    @BeforeEach
    void setUp() {
        controller = new SqsBackpressureController(lagMetrics, meterRegistry);
        setField(controller, "lagPauseThreshold", 10_000L);
        setField(controller, "lagResumeThreshold", 1_000L);
    }

    @Test
    void pausesWhenQueueDepthExceedsPauseThreshold() {
        when(lagMetrics.getMaxDepth()).thenReturn(12_000L);

        controller.checkAndAdjust();

        assertThat(controller.isPaused()).isTrue();
        assertThat(gaugeValue()).isEqualTo(1.0);
    }

    @Test
    void resumesWhenQueueDepthDropsBelowResumeThreshold() {
        when(lagMetrics.getMaxDepth()).thenReturn(12_000L);
        controller.checkAndAdjust();
        assertThat(controller.isPaused()).isTrue();

        when(lagMetrics.getMaxDepth()).thenReturn(500L);
        controller.checkAndAdjust();

        assertThat(controller.isPaused()).isFalse();
        assertThat(gaugeValue()).isEqualTo(0.0);
    }

    @Test
    void remainsPausedWhenDepthBetweenThresholds() {
        when(lagMetrics.getMaxDepth()).thenReturn(12_000L);
        controller.checkAndAdjust();

        // Depth between resume (1000) and pause (10000) thresholds — stays paused
        when(lagMetrics.getMaxDepth()).thenReturn(5_000L);
        controller.checkAndAdjust();

        assertThat(controller.isPaused()).isTrue();
    }

    @Test
    void doesNotPauseWhenDepthBelowPauseThreshold() {
        when(lagMetrics.getMaxDepth()).thenReturn(500L);

        controller.checkAndAdjust();

        assertThat(controller.isPaused()).isFalse();
        assertThat(gaugeValue()).isEqualTo(0.0);
    }

    private double gaugeValue() {
        return meterRegistry.find("consumer.backpressure.active").gauge().value();
    }

    private static void setField(Object target, String fieldName, Object value) {
        try {
            var field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
