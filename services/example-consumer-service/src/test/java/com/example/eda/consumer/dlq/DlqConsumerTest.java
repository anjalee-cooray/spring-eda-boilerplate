package com.example.eda.consumer.dlq;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class DlqConsumerTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final DlqConsumer consumer = new DlqConsumer(meterRegistry);

    @Test
    void incrementsCounterForEachMessage() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("example.created.dlq", 0, 0, null, "{\"bad\":\"event\"}");

        consumer.onDeadLetter(record, "something went wrong", "java.lang.RuntimeException",
                "example.created".getBytes(), "0".getBytes(), "42".getBytes());

        double count = meterRegistry.counter("events.dlq.received", "topic", "example.created.dlq").count();
        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void incrementsPerTopicNotShared() {
        ConsumerRecord<String, String> r1 = new ConsumerRecord<>("topic-a.dlq", 0, 0, null, "{}");
        ConsumerRecord<String, String> r2 = new ConsumerRecord<>("topic-b.dlq", 0, 0, null, "{}");

        consumer.onDeadLetter(r1, null, null, null, null, null);
        consumer.onDeadLetter(r2, null, null, null, null, null);
        consumer.onDeadLetter(r1, null, null, null, null, null);

        assertThat(meterRegistry.counter("events.dlq.received", "topic", "topic-a.dlq").count()).isEqualTo(2.0);
        assertThat(meterRegistry.counter("events.dlq.received", "topic", "topic-b.dlq").count()).isEqualTo(1.0);
    }

    @Test
    void doesNotThrowOnNullHeaders() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("example.created.dlq", 0, 0, null, "payload");

        assertThatCode(() -> consumer.onDeadLetter(record, null, null, null, null, null))
                .doesNotThrowAnyException();
    }
}
