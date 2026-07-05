package com.example.eda.consumer.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

/**
 * Stops all Kafka listener containers before the Spring context closes.
 *
 * Without explicit coordination, Spring may close the datasource or other
 * infrastructure beans while a Kafka listener is still mid-message. This bean
 * runs at a higher phase than KafkaBackpressureController (which is also a
 * SmartLifecycle) so it fires first, ensuring Kafka consumers finish any
 * in-progress handler before the rest of the context tears down.
 *
 * Combined with server.shutdown=graceful and spring.lifecycle.timeout-per-
 * shutdown-phase=30s, this guarantees:
 *
 *   1. HTTP server stops accepting new requests
 *   2. This bean calls registry.stop() — listeners drain current messages
 *   3. timeout-per-shutdown-phase (30s) bounds the drain window
 *   4. Remaining beans (datasource, outbox writer, etc.) close cleanly
 *
 * Only active when app.events.broker=kafka.
 */
@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "kafka", matchIfMissing = true)
public class KafkaConsumerShutdownHandler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerShutdownHandler.class);

    // Higher than KafkaBackpressureController so this bean's stop() fires first
    private static final int PHASE = Integer.MAX_VALUE - 50;

    private final KafkaListenerEndpointRegistry registry;
    private volatile boolean running = false;

    public KafkaConsumerShutdownHandler(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        log.info("KafkaConsumerShutdownHandler stopping — pausing all Kafka listener containers");
        registry.stop();
        running = false;
        log.info("All Kafka listener containers stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
