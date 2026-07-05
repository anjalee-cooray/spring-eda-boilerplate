package com.example.eda.db.eventstore;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Periodically moves old event_store records to event_store_archive.
 *
 * Why archival matters:
 *   The event_store grows unbounded. Without archival, full-history replays and
 *   audit queries grow slower every year and index pages bloat. Archiving old rows
 *   keeps the hot table small without losing any data.
 *
 * Archival strategy:
 *   INSERT INTO event_store_archive ... SELECT ... FROM event_store WHERE occurred_at < cutoff
 *   DELETE FROM event_store WHERE occurred_at < cutoff
 *
 *   Both run in a single transaction per batch so the move is atomic:
 *   a record is either in event_store or event_store_archive, never in both,
 *   never in neither. The archival job is idempotent — re-running is safe because
 *   event_store_archive does not have a UNIQUE constraint on event_id.
 *
 * Configuration:
 *   app.event-store.archive-after-days  — how old a record must be before archival (default 365)
 *   app.event-store.archive-batch-size  — max rows per archival run (default 10 000)
 *   app.event-store.archive-cron        — cron schedule (default: 2am UTC daily)
 *
 * Monitoring:
 *   event_store.archived.rows counter — tracks cumulative rows archived.
 *   Alert if this counter is 0 for 7 days (archival may be stuck).
 *
 * Recovery:
 *   To replay an archived event, SELECT from event_store_archive with the same
 *   filters used in EventStoreRepository.findForReplay(), or INSERT the rows
 *   back into event_store temporarily if the replay infrastructure needs them.
 */
@Component
public class EventStoreArchivalJob {

    private static final Logger log = LoggerFactory.getLogger(EventStoreArchivalJob.class);

    private final JdbcTemplate jdbc;
    private final Counter archivedCounter;

    @Value("${app.event-store.archive-after-days:365}")
    private int archiveAfterDays;

    @Value("${app.event-store.archive-batch-size:10000}")
    private int batchSize;

    public EventStoreArchivalJob(JdbcTemplate jdbc, MeterRegistry meterRegistry) {
        this.jdbc = jdbc;
        this.archivedCounter = Counter.builder("event_store.archived.rows")
                .description("Total event_store rows moved to event_store_archive")
                .register(meterRegistry);
    }

    @Scheduled(cron = "${app.event-store.archive-cron:0 0 2 * * *}")
    @Transactional
    public void archive() {
        Instant cutoff = Instant.now().minus(archiveAfterDays, ChronoUnit.DAYS);

        log.info("EventStoreArchivalJob starting archiveAfterDays={} cutoff={} batchSize={}",
                archiveAfterDays, cutoff, batchSize);

        // Step 1: copy eligible rows to archive
        int copied = jdbc.update("""
                INSERT INTO event_store_archive
                    (id, event_id, event_type, tenant_id, aggregate_id, aggregate_type,
                     schema_version, payload, correlation_id, causation_id, occurred_at)
                SELECT id, event_id, event_type, tenant_id, aggregate_id, aggregate_type,
                       schema_version, payload, correlation_id, causation_id, occurred_at
                FROM event_store
                WHERE occurred_at < ?
                LIMIT ?
                """, cutoff, batchSize);

        if (copied == 0) {
            log.debug("EventStoreArchivalJob: no rows eligible for archival");
            return;
        }

        // Step 2: delete original rows in the same transaction
        int deleted = jdbc.update("""
                DELETE FROM event_store
                WHERE id IN (
                    SELECT id FROM event_store
                    WHERE occurred_at < ?
                    LIMIT ?
                )
                """, cutoff, batchSize);

        archivedCounter.increment(deleted);

        log.info("EventStoreArchivalJob complete: archived={} deleted={} cutoff={}",
                copied, deleted, cutoff);
    }
}
