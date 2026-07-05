package com.example.eda.events.schema;

import java.util.Map;

/**
 * Registry of current schema versions per event type.
 *
 * When you bump a schema version:
 *   1. Increment the version constant here.
 *   2. Add the new JSON Schema file: schemas/{event_type}-v{N}.json
 *   3. Keep the old schema file — the EventUpcaster chain handles consumers
 *      that receive events published under the old version.
 *
 * Backward-compatible changes (adding optional fields) do not require a version bump.
 * Breaking changes (removing fields, changing types, renaming) always require one.
 */
public final class EventVersion {

    public static final String V1 = "1";
    public static final String V2 = "2";

    // Current schema version per event type. Add your event types here.
    private static final Map<String, String> CURRENT = Map.of(
            "example.created", V1,
            "example.updated", V1
    );

    private EventVersion() {}

    /**
     * Returns the current schema version for an event type.
     * Defaults to "1" for event types not explicitly registered.
     */
    public static String current(String eventType) {
        return CURRENT.getOrDefault(eventType, V1);
    }
}
