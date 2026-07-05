package com.example.eda.relay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Coordinates graceful shutdown of the outbox relay.
 *
 * When a SIGTERM arrives, Spring calls stop() on all SmartLifecycle beans in
 * reverse phase order before closing the application context. This bean runs at
 * a high phase number so it stops before most infrastructure beans, giving the
 * relay a window to finish the batch it is currently processing.
 *
 * Flow:
 *   1. SIGTERM received → Spring calls stop() on this bean
 *   2. pollingEnabled set to false — OutboxRelayPoller.poll() returns early on
 *      next tick, skipping any new claim
 *   3. Any in-progress batch completes normally (Phase 2 + Phase 3 finish)
 *   4. spring.lifecycle.timeout-per-shutdown-phase (30s) bounds the wait
 *   5. Remaining beans shut down (datasource, Kafka producer, etc.)
 *
 * Why not @PreDestroy?
 *   @PreDestroy fires after the context starts closing — datasource and broker
 *   beans may already be unavailable. SmartLifecycle.stop() fires earlier, while
 *   all dependencies are still live.
 */
@Component
public class OutboxRelayShutdownCoordinator implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayShutdownCoordinator.class);

    // High phase → this bean's stop() runs early in the shutdown sequence,
    // before infrastructure beans (datasource, Kafka) are torn down.
    private static final int PHASE = Integer.MAX_VALUE - 100;

    private volatile boolean pollingEnabled = false;
    private volatile boolean running = false;

    @Override
    public void start() {
        pollingEnabled = true;
        running = true;
        log.info("OutboxRelayShutdownCoordinator started — polling enabled");
    }

    @Override
    public void stop() {
        log.info("OutboxRelayShutdownCoordinator stopping — disabling outbox polling");
        pollingEnabled = false;
        running = false;
        log.info("Outbox polling disabled — in-progress batch will complete before shutdown");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    /** Returns false when shutdown has been signalled — OutboxRelayPoller checks this. */
    public boolean isPollingEnabled() {
        return pollingEnabled;
    }
}
