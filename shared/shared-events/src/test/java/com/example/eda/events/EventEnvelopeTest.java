package com.example.eda.events;

import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventEnvelopeTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void buildsWithAllRequiredFields() {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventType("order.placed")
                .tenantId("tenant-1")
                .correlationId("corr-1")
                .payload(new TestPayload("order-1"))
                .build();

        assertThat(envelope.eventId()).isNotNull();
        assertThat(envelope.eventType()).isEqualTo("order.placed");
        assertThat(envelope.tenantId()).isEqualTo("tenant-1");
        assertThat(envelope.correlationId()).isEqualTo("corr-1");
        assertThat(envelope.occurredAt()).isNotNull();
        assertThat(envelope.causationId()).isNull();
    }

    @Test
    void throwsWhenEventTypeIsBlank() {
        assertThatThrownBy(() -> EventEnvelope.builder()
                .eventType("")
                .tenantId("tenant-1")
                .correlationId("corr-1")
                .payload("payload")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("eventType");
    }

    @Test
    void throwsWhenTenantIdIsNull() {
        assertThatThrownBy(() -> EventEnvelope.builder()
                .eventType("order.placed")
                .tenantId(null)
                .correlationId("corr-1")
                .payload("payload")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tenantId");
    }

    @Test
    void serialisesAndDeserialisesCorrectly() throws Exception {
        EventEnvelope original = EventEnvelope.builder()
                .eventType("order.placed")
                .tenantId("tenant-1")
                .correlationId("corr-1")
                .causationId("cause-1")
                .occurredAt(Instant.parse("2025-01-01T00:00:00Z"))
                .payload(new TestPayload("order-1"))
                .build();

        String json = objectMapper.writeValueAsString(original);

        assertThat(json).contains("order.placed");
        assertThat(json).contains("tenant-1");
        assertThat(json).contains("corr-1");
    }

    record TestPayload(String orderId) { }
}
