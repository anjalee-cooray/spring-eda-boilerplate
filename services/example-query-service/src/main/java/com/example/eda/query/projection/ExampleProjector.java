package com.example.eda.query.projection;

import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class ExampleProjector implements EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ExampleProjector.class);

    private final ExampleReadModelRepository repository;
    private final ObjectMapper objectMapper;

    public ExampleProjector(ExampleReadModelRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean supports(String eventType) {
        return "example.created".equals(eventType);
    }

    @Override
    @Transactional
    public void handle(EventEnvelope envelope) {
        log.debug("Projecting event type={} eventId={}", envelope.eventType(), envelope.eventId());

        Map<?, ?> payload = objectMapper.convertValue(envelope.payload(), Map.class);
        UUID id = UUID.fromString(payload.get("id").toString());
        String name = payload.get("name").toString();

        ExampleReadModel readModel = ExampleReadModel.builder()
                .id(id)
                .tenantId(envelope.tenantId())
                .name(name)
                .status("CREATED")
                .build();

        repository.save(readModel);
    }
}
