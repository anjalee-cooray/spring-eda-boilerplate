package com.example.eda.events.publisher;

import com.example.eda.events.envelope.EventEnvelope;

/**
 * Pluggable event publishing interface.
 * Swap implementations via Spring conditional beans — Kafka or SNS/SQS.
 */
public interface EventPublisher {

    void publish(EventEnvelope envelope);

    void publish(EventEnvelope envelope, String destination);
}
