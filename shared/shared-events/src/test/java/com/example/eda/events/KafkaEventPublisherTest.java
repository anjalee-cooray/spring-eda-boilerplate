package com.example.eda.events;

import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.kafka.KafkaEventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaEventPublisherTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final KafkaEventPublisher publisher = new KafkaEventPublisher(kafkaTemplate, objectMapper);

    @Test
    void publishesToTopicNamedAfterEventType() {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventType("order.placed")
                .tenantId("tenant-1")
                .correlationId("corr-1")
                .payload("payload")
                .build();

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        publisher.publish(envelope);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), any());

        assertThat(topicCaptor.getValue()).isEqualTo("order.placed");
        assertThat(keyCaptor.getValue()).isEqualTo("tenant-1");
    }

    @Test
    void publishesToExplicitDestination() {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventType("order.placed")
                .tenantId("tenant-1")
                .correlationId("corr-1")
                .payload("payload")
                .build();

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        publisher.publish(envelope, "custom-topic");

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), anyString(), any());

        assertThat(topicCaptor.getValue()).isEqualTo("custom-topic");
    }
}
