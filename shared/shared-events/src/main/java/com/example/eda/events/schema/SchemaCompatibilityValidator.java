package com.example.eda.events.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Validates on startup that every registered EventUpcaster chain is contiguous —
 * no version gaps that would leave an event stranded mid-chain.
 *
 * Example of a chain gap (bad):
 *   ExampleCreatedV1ToV2Upcaster  (1→2) registered ✅
 *   ExampleCreatedV3ToV4Upcaster  (3→4) registered ✅
 *   ExampleCreatedV2ToV3Upcaster  (2→3) MISSING    ❌
 *
 *   An event published at v1 would reach v2 then stop — v3 and v4 handlers would
 *   never see it. This validator detects the gap at startup and fails fast rather
 *   than silently processing events at stale versions in production.
 *
 * This runs after the application context is fully initialised (ApplicationReadyEvent)
 * so all @Component upcasters are in the registry before the check runs.
 *
 * Blue/Green deployment guidance — see SCHEMA_MIGRATION_RUNBOOK.md in the repo root.
 */
@Component
public class SchemaCompatibilityValidator {

    private static final Logger log = LoggerFactory.getLogger(SchemaCompatibilityValidator.class);

    private final EventUpcasterRegistry upcasterRegistry;

    public SchemaCompatibilityValidator(EventUpcasterRegistry upcasterRegistry) {
        this.upcasterRegistry = upcasterRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void validate() {
        Map<String, List<EventUpcaster>> byType = upcasterRegistry.getAllUpcasters();

        if (byType.isEmpty()) {
            log.debug("SchemaCompatibilityValidator: no upcasters registered — skipping chain validation");
            return;
        }

        boolean hasGap = false;

        for (Map.Entry<String, List<EventUpcaster>> entry : byType.entrySet()) {
            String eventType = entry.getKey();

            // Sort by fromVersion ascending (versions are string integers like "1", "2")
            List<EventUpcaster> sorted = entry.getValue().stream()
                    .sorted((a, b) -> {
                        int va = Integer.parseInt(a.fromVersion());
                        int vb = Integer.parseInt(b.fromVersion());
                        return Integer.compare(va, vb);
                    })
                    .toList();

            // Each upcaster's toVersion must equal the next one's fromVersion
            for (int i = 0; i < sorted.size() - 1; i++) {
                EventUpcaster current = sorted.get(i);
                EventUpcaster next    = sorted.get(i + 1);

                if (!current.toVersion().equals(next.fromVersion())) {
                    log.error("Schema upcaster chain gap! eventType={} missing v{}→v{} "
                                    + "(have v{}→v{} and v{}→v{})",
                            eventType,
                            current.toVersion(), next.fromVersion(),
                            current.fromVersion(), current.toVersion(),
                            next.fromVersion(), next.toVersion());
                    hasGap = true;
                }
            }

            log.info("SchemaCompatibilityValidator: eventType={} chain=[{}]",
                    eventType,
                    sorted.stream()
                            .map(u -> "v" + u.fromVersion() + "→v" + u.toVersion())
                            .collect(Collectors.joining(", ")));
        }

        if (hasGap) {
            throw new IllegalStateException(
                    "Upcaster chain gaps detected — service will process older event versions incorrectly. "
                    + "Register missing upcasters or see SCHEMA_MIGRATION_RUNBOOK.md.");
        }

        log.info("SchemaCompatibilityValidator: all upcaster chains contiguous ✓");
    }
}
