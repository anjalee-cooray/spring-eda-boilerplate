package com.example.eda.events.schema;

import java.util.Set;

/**
 * Thrown when an event payload fails JSON Schema validation before publishing.
 * This is a programming error — fix the producer, not the schema.
 */
public class SchemaValidationException extends RuntimeException {

    public SchemaValidationException(String eventType, String schemaVersion, Set<String> errors) {
        super("Event payload for type=%s version=%s failed schema validation: %s"
                .formatted(eventType, schemaVersion, errors));
    }
}
