package com.example.eda.db.saga;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Detects saga instances that have been STARTED for longer than the configured TTL
 * and marks them TIMED_OUT.
 *
 * Why this matters:
 *   If a participant never responds (network partition, service down, message lost),
 *   the saga stays in STARTED forever and holds a partially completed booking/order
 *   in a limbo state. This task clears that state automatically and increments a
 *   metric so operators can alert on saga timeout spikes.
 *
 * Compensation:
 *   This task only marks TIMED_OUT — it does not trigger compensation automatically.
 *   Compensation for timed-out sagas is domain-specific (some flows need to reverse,
 *   others need human review). Implement a SagaTimeoutHandler per saga type that
 *   listens to a saga.timed-out event or polls TIMED_OUT instances and acts.
 *
 *   For automatic compensation, extend each saga's EventConsumer to also handle
 *   "saga.timed-out" events, or use Temporal/Axon which handle timeouts natively.
 *
 * Configuration:
 *   app.saga.timeout-minutes — how long a STARTED saga may stay alive (default 60 min)
 *   app.saga.timeout-check-interval — how often this task runs (default 5 min)
 */
@Component
public class SagaTimeoutTask {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutTask.class);

    private final SagaRepository sagaRepository;
    private final Counter timedOutCounter;

    @Value("${app.saga.timeout-minutes:60}")
    private int timeoutMinutes;

    public SagaTimeoutTask(SagaRepository sagaRepository, MeterRegistry meterRegistry) {
        this.sagaRepository = sagaRepository;
        this.timedOutCounter = Counter.builder("saga.instances.timed-out")
                .description("Saga instances that exceeded the max wait time and were marked TIMED_OUT")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${app.saga.timeout-check-interval-ms:300000}")
    @Transactional
    public void checkTimeouts() {
        Instant cutoff = Instant.now().minusSeconds(timeoutMinutes * 60L);

        List<SagaInstance> stale = sagaRepository.findStartedBefore(cutoff);
        if (stale.isEmpty()) return;

        log.warn("SagaTimeoutTask found {} stale saga(s) older than {} minutes", stale.size(), timeoutMinutes);

        for (SagaInstance saga : stale) {
            saga.markTimedOut("Saga exceeded timeout of " + timeoutMinutes + " minutes at step: " + saga.getCurrentStep());
            sagaRepository.save(saga);
            timedOutCounter.increment();

            log.error("Saga timed out sagaType={} correlationId={} step={} startedAt={}",
                    saga.getSagaType(), saga.getCorrelationId(), saga.getCurrentStep(), saga.getCreatedAt());
        }
    }
}
