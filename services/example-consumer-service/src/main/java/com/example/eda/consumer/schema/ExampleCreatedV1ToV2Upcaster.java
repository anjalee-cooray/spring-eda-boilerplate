package com.example.eda.consumer.schema;

import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.schema.EventUpcaster;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Example upcaster: upgrades an example.created event from schema v1 to v2.
 *
 * In this hypothetical v2 migration a new required field "displayName" was added.
 * Events produced before the schema bump lack this field, so the upcaster
 * synthesises a default value from the existing "name" field.
 *
 * HOW TO REGISTER A NEW UPCASTER:
 *   1. Bump the version in EventVersion: "example.created" → "2"
 *   2. Add the new JSON Schema file: schemas/example.created-v2.json
 *   3. Implement EventUpcaster (fromVersion="1", toVersion="2") like this class
 *   4. Annotate with @Component — EventUpcasterRegistry picks it up automatically
 *
 * This class is intentionally commented out in production. Uncomment it (or copy
 * it as a template) when you introduce a v2 schema for example.created.
 *
 * NOTE: This class is registered as a @Component only as a demonstration.
 * In a real service you would uncomment it only when a v2 schema actually exists.
 */
// @Component  ← uncomment when you introduce example.created v2
public class ExampleCreatedV1ToV2Upcaster implements EventUpcaster {

    @Override
    public String eventType() {
        return "example.created";
    }

    @Override
    public String fromVersion() {
        return "1";
    }

    @Override
    public String toVersion() {
        return "2";
    }

    @Override
    @SuppressWarnings("unchecked")
    public EventEnvelope upcast(EventEnvelope envelope) {
        // Safe cast: Kafka/SQS deserialises payload as Map<String, Object> via Jackson
        Map<String, Object> oldPayload = (Map<String, Object>) envelope.payload();

        // Create a new payload map — never mutate the original (other handlers may share it)
        Map<String, Object> newPayload = new LinkedHashMap<>(oldPayload);

        // Synthesise the new required field from existing data
        newPayload.putIfAbsent("displayName", oldPayload.getOrDefault("name", ""));

        return EventEnvelope.builder()
                .eventType(envelope.eventType())
                .tenantId(envelope.tenantId())
                .correlationId(envelope.correlationId())
                .causationId(envelope.causationId())
                .occurredAt(envelope.occurredAt())
                .payload(newPayload)
                .schemaVersion(toVersion())
                .build();
    }
}
