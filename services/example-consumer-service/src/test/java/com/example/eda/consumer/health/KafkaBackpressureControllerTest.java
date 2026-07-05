package com.example.eda.consumer.health;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaBackpressureControllerTest {

    private final KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
    private final KafkaConsumerLagMetrics lagMetrics = mock(KafkaConsumerLagMetrics.class);
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private final MessageListenerContainer container = mock(MessageListenerContainer.class);

    private KafkaBackpressureController controller;

    @BeforeEach
    void setUp() {
        when(registry.getListenerContainers())
                .thenReturn((Collection) List.of(container));

        controller = new KafkaBackpressureController(registry, lagMetrics, meterRegistry);
        // Force @Value fields using reflection since we are not in a Spring context
        setField(controller, "lagPauseThreshold", 10_000L);
        setField(controller, "lagResumeThreshold", 1_000L);
    }

    @Test
    void pausesContainersWhenLagExceedsThreshold() {
        when(lagMetrics.getMaxLag()).thenReturn(15_000L);

        controller.checkAndAdjust();

        verify(container).pause();
        assertThat(gaugeValue()).isEqualTo(1.0);
    }

    @Test
    void resumesContainersWhenLagDropsBelowResumeThreshold() {
        // First — pause
        when(lagMetrics.getMaxLag()).thenReturn(15_000L);
        controller.checkAndAdjust();

        // Then — resume
        when(lagMetrics.getMaxLag()).thenReturn(500L);
        controller.checkAndAdjust();

        verify(container).resume();
        assertThat(gaugeValue()).isEqualTo(0.0);
    }

    @Test
    void doesNotPauseWhenLagIsBelowThreshold() {
        when(lagMetrics.getMaxLag()).thenReturn(500L);

        controller.checkAndAdjust();

        verify(container, never()).pause();
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
