package com.example.eda.relay.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

/**
 * Runtime-refreshable configuration for the outbox relay.
 *
 * Relay tuning knobs (batch size, poll interval) are read by OutboxRelayPoller
 * via @Value fields. To make them truly dynamic without a restart, inject this
 * bean into OutboxRelayPoller and call getBatchSize() / getPollIntervalMs() in
 * poll() instead of using @Value fields directly on the poller.
 */
@Component
@RefreshScope
public class DynamicRelayConfig {

    private static final Logger log = LoggerFactory.getLogger(DynamicRelayConfig.class);

    @Value("${app.relay.batch-size:50}")
    private int batchSize;

    @Value("${app.relay.max-attempts:5}")
    private int maxAttempts;

    @Value("${app.relay.lock-timeout-seconds:30}")
    private int lockTimeoutSeconds;

    @Value("${app.features.replay-enabled:true}")
    private boolean replayEnabled;

    public int getBatchSize() { return batchSize; }
    public int getMaxAttempts() { return maxAttempts; }
    public int getLockTimeoutSeconds() { return lockTimeoutSeconds; }
    public boolean isReplayEnabled() { return replayEnabled; }

    @jakarta.annotation.PostConstruct
    public void logConfig() {
        log.info("DynamicRelayConfig loaded: batchSize={} maxAttempts={} replayEnabled={}",
                batchSize, maxAttempts, replayEnabled);
    }
}
