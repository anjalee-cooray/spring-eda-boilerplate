package com.example.eda.events.schema;

import com.example.eda.events.envelope.EventEnvelope;

/**
 * Upgrades an event from one schema version to the next.
 *
 * Implement this interface to handle a v1→v2 migration for a specific event type.
 * Register your implementation as a Spring bean — EventUpcasterRegistry picks it up
 * automatically and applies the chain in version order before event handlers run.
 *
 * Example — adding a required field in v2:
 *
 *   @Component
 *   public class ExampleCreatedV1ToV2Upcaster implements EventUpcaster {
 *       public String eventType()   { return "example.created"; }
 *       public String fromVersion() { return "1"; }
 *       public String toVersion()   { return "2"; }
 *
 *       public EventEnvelope upcast(EventEnvelope envelope) {
 *           Map<String, Object> payload = (Map<String, Object>) envelope.payload();
 *           payload.put("newRequiredField", "default-value");
 *           return EventEnvelope.builder()
 *                   .eventId(envelope.eventId())   // preserve identity fields
 *                   ...
 *                   .schemaVersion(toVersion())
 *                   .build();
 *       }
 *   }
 *
 * Rules:
 *   - fromVersion() and toVersion() must be adjacent (1→2, not 1→3)
 *   - Never mutate the original envelope — always return a new one
 *   - Upcasters run in version order: v1→v2 runs before v2→v3
 *   - An upcaster is only called when the incoming event's schema_version matches fromVersion()
 */
public interface EventUpcaster {

    String eventType();

    String fromVersion();

    String toVersion();

    EventEnvelope upcast(EventEnvelope envelope);
}
