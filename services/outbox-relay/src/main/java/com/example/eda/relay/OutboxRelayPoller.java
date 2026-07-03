package com.example.eda.relay;

import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxRepository;
import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.publisher.EventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxRelayPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayPoller.class);

    private final OutboxRepository outboxRepository;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    @Value("${app.relay.batch-size:50}")
    private int batchSize;

    public OutboxRelayPoller(
            OutboxRepository outboxRepository,
            EventPublisher eventPublisher,
            ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${app.relay.poll-interval-ms:1000}")
    @Transactional
    public void poll() {
        List<OutboxRecord> pending = outboxRepository.findPendingRecords(batchSize);

        if (pending.isEmpty()) {
            return;
        }

        log.debug("Relaying {} outbox records", pending.size());

        for (OutboxRecord record : pending) {
            try {
                EventEnvelope envelope = buildEnvelope(record);
                eventPublisher.publish(envelope);
                record.markPublished();
                outboxRepository.save(record);
            } catch (Exception ex) {
                log.error("Failed to relay outbox record id={} eventType={} tenantId={}",
                        record.getId(), record.getEventType(), record.getTenantId(), ex);
            }
        }
    }

    private EventEnvelope buildEnvelope(OutboxRecord record) {
        Object payload = deserializePayload(record.getPayload());
        return EventEnvelope.builder()
                .eventType(record.getEventType())
                .tenantId(record.getTenantId())
                .correlationId(record.getCorrelationId() != null ? record.getCorrelationId() : record.getEventId().toString())
                .payload(payload)
                .build();
    }

    private Object deserializePayload(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson, Object.class);
        } catch (Exception e) {
            log.warn("Could not deserialize payload as JSON, relaying as raw string");
            return payloadJson;
        }
    }
}
