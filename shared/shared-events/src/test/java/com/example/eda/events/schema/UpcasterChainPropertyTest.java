package com.example.eda.events.schema;

import com.example.eda.events.envelope.EventEnvelope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property / invariant tests for EventUpcasterRegistry and SchemaCompatibilityValidator.
 *
 * Each test encodes a correctness property of the upcaster chain that must hold
 * regardless of which upcasters are registered or in what order:
 *
 *   P1 - single upcaster transforms the event to the next version
 *   P2 - multi-step chain is applied transitively in a single call
 *   P3 - registration order does not affect chain execution order
 *   P4 - event already at latest version is returned unchanged
 *   P5 - empty registry returns the original envelope
 *   P6 - upcasters for unknown event types do not apply to other event types
 *   P7 - multiple event types are upcasted independently without cross-contamination
 *   P8 - contiguous chain passes SchemaCompatibilityValidator
 *   P9 - chain with a gap fails SchemaCompatibilityValidator with a clear message
 *   P10 - single-upcaster chain always passes SchemaCompatibilityValidator (no internal gap possible)
 */
class UpcasterChainPropertyTest {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Builds a test upcaster that copies the envelope, bumps schemaVersion, and adds a marker field. */
    static EventUpcaster upcaster(String eventType, String fromVersion, String toVersion, String markerField) {
        return new EventUpcaster() {
            @Override public String eventType()   { return eventType; }
            @Override public String fromVersion() { return fromVersion; }
            @Override public String toVersion()   { return toVersion; }

            @Override
            public EventEnvelope upcast(EventEnvelope envelope) {
                @SuppressWarnings("unchecked")
                Map<String, Object> payload = new LinkedHashMap<>((Map<String, Object>) envelope.payload());
                payload.put(markerField, "added-in-v" + toVersion);
                return new EventEnvelope(
                        envelope.eventId(), envelope.eventType(), envelope.tenantId(),
                        envelope.correlationId(), envelope.causationId(), envelope.occurredAt(),
                        payload, toVersion, envelope.aggregateId());
            }
        };
    }

    static EventEnvelope envelopeAt(String eventType, String version) {
        return EventEnvelope.builder()
                .eventType(eventType)
                .tenantId("tenant-prop-test")
                .correlationId("corr-prop-test")
                .schemaVersion(version)
                .occurredAt(Instant.now())
                .payload(new LinkedHashMap<>(Map.of("id", "entity-1")))
                .build();
    }

    // -------------------------------------------------------------------------
    // P1 — single upcaster transforms v1 → v2
    // -------------------------------------------------------------------------

    @Test
    void p1_singleUpcaster_transformsEnvelopeToNextVersion() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(
                List.of(upcaster("example.created", "1", "2", "fieldV2")));

        EventEnvelope result = registry.upcastToLatest(envelopeAt("example.created", "1"));

        assertThat(result.schemaVersion()).isEqualTo("2");
        assertThat(((Map<?, ?>) result.payload())).containsKey("fieldV2");
    }

    // -------------------------------------------------------------------------
    // P2 — multi-step chain is applied transitively in a single call
    // -------------------------------------------------------------------------

    @Test
    void p2_twoUpcasterChain_appliesBothStepsInOneCall() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(List.of(
                upcaster("example.created", "1", "2", "fieldV2"),
                upcaster("example.created", "2", "3", "fieldV3")));

        EventEnvelope result = registry.upcastToLatest(envelopeAt("example.created", "1"));

        assertThat(result.schemaVersion()).isEqualTo("3");
        assertThat(((Map<?, ?>) result.payload())).containsKeys("fieldV2", "fieldV3");
    }

    @Test
    void p2_threeUpcasterChain_appliesAllThreeSteps() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(List.of(
                upcaster("example.created", "1", "2", "f2"),
                upcaster("example.created", "2", "3", "f3"),
                upcaster("example.created", "3", "4", "f4")));

        EventEnvelope result = registry.upcastToLatest(envelopeAt("example.created", "1"));

        assertThat(result.schemaVersion()).isEqualTo("4");
        assertThat(((Map<?, ?>) result.payload())).containsKeys("f2", "f3", "f4");
    }

    // -------------------------------------------------------------------------
    // P3 — registration order does not affect execution order
    // -------------------------------------------------------------------------

    @Test
    void p3_reverseRegistrationOrder_chainStillAppliesInVersionOrder() {
        // Register v2→v3 before v1→v2 — chain must still run v1→v2 first
        EventUpcasterRegistry registry = new EventUpcasterRegistry(List.of(
                upcaster("example.created", "2", "3", "fieldV3"),
                upcaster("example.created", "1", "2", "fieldV2")));

        EventEnvelope result = registry.upcastToLatest(envelopeAt("example.created", "1"));

        assertThat(result.schemaVersion()).isEqualTo("3");
        // Both fields added — order enforced by registry chain traversal, not registration order
        assertThat(((Map<?, ?>) result.payload())).containsKeys("fieldV2", "fieldV3");
    }

    // -------------------------------------------------------------------------
    // P4 — event already at latest version is returned unchanged (same reference)
    // -------------------------------------------------------------------------

    @Test
    void p4_eventAtLatestVersion_returnedUnchanged() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(
                List.of(upcaster("example.created", "1", "2", "fieldV2")));

        EventEnvelope v2 = envelopeAt("example.created", "2"); // already latest
        EventEnvelope result = registry.upcastToLatest(v2);

        assertThat(result).isSameAs(v2);
        assertThat(result.schemaVersion()).isEqualTo("2");
    }

    // -------------------------------------------------------------------------
    // P5 — empty registry returns original envelope
    // -------------------------------------------------------------------------

    @Test
    void p5_emptyRegistry_returnsOriginalEnvelopeUnchanged() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(List.of());

        EventEnvelope original = envelopeAt("example.created", "1");
        EventEnvelope result = registry.upcastToLatest(original);

        assertThat(result).isSameAs(original);
    }

    // -------------------------------------------------------------------------
    // P6 — upcasters for unknown event type do not apply
    // -------------------------------------------------------------------------

    @Test
    void p6_upcasterForDifferentEventType_doesNotApply() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(
                List.of(upcaster("other.event", "1", "2", "field")));

        EventEnvelope original = envelopeAt("example.created", "1");
        EventEnvelope result = registry.upcastToLatest(original);

        assertThat(result).isSameAs(original); // "example.created" has no upcasters
    }

    // -------------------------------------------------------------------------
    // P7 — multiple event types upcasted independently, no cross-contamination
    // -------------------------------------------------------------------------

    @Test
    void p7_multipleEventTypes_eachChainAppliedIndependently() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(List.of(
                upcaster("example.created", "1", "2", "createdV2"),
                upcaster("example.updated", "1", "2", "updatedV2")));

        EventEnvelope createdResult = registry.upcastToLatest(envelopeAt("example.created", "1"));
        EventEnvelope updatedResult = registry.upcastToLatest(envelopeAt("example.updated", "1"));

        assertThat(createdResult.schemaVersion()).isEqualTo("2");
        assertThat(updatedResult.schemaVersion()).isEqualTo("2");
        assertThat(((Map<?, ?>) createdResult.payload()))
                .containsKey("createdV2")
                .doesNotContainKey("updatedV2");
        assertThat(((Map<?, ?>) updatedResult.payload()))
                .containsKey("updatedV2")
                .doesNotContainKey("createdV2");
    }

    // -------------------------------------------------------------------------
    // P8 — contiguous chain passes validator
    // -------------------------------------------------------------------------

    @Test
    void p8_contiguousChain_validatorPasses() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(List.of(
                upcaster("example.created", "1", "2", "f"),
                upcaster("example.created", "2", "3", "g")));

        SchemaCompatibilityValidator validator = new SchemaCompatibilityValidator(registry);

        validator.validate(); // must not throw
    }

    // -------------------------------------------------------------------------
    // P9 — chain with gap fails validator with a clear message
    // -------------------------------------------------------------------------

    @Test
    void p9_gapInChain_validatorThrowsIllegalStateWithDescription() {
        // v1→v2 and v3→v4 registered; v2→v3 is missing
        EventUpcasterRegistry registry = new EventUpcasterRegistry(List.of(
                upcaster("example.created", "1", "2", "f"),
                upcaster("example.created", "3", "4", "h")));

        SchemaCompatibilityValidator validator = new SchemaCompatibilityValidator(registry);

        assertThatThrownBy(validator::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Upcaster chain gaps detected");
    }

    // -------------------------------------------------------------------------
    // P10 — single-upcaster chain has no internal gaps (trivially contiguous)
    // -------------------------------------------------------------------------

    @Test
    void p10_singleUpcaster_validatorAlwaysPasses() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(
                List.of(upcaster("example.created", "5", "6", "f")));

        SchemaCompatibilityValidator validator = new SchemaCompatibilityValidator(registry);

        validator.validate(); // must not throw — no inter-step gaps possible with one upcaster
    }

    @Test
    void p10_emptyRegistry_validatorSkipsAndPasses() {
        EventUpcasterRegistry registry = new EventUpcasterRegistry(List.of());
        SchemaCompatibilityValidator validator = new SchemaCompatibilityValidator(registry);

        validator.validate(); // empty registry — nothing to validate
    }
}
