package com.example.eda.events.consumer;

import com.example.eda.events.envelope.EventEnvelope;

/**
 * Pluggable event consumption interface.
 * Implement this to handle a specific event type — the consumer infrastructure
 * (Kafka listener or SQS poller) calls handle() after deserialising the envelope.
 */
public interface EventConsumer {

    void handle(EventEnvelope envelope);

    default boolean supports(String eventType) {
        return true;
    }
}
