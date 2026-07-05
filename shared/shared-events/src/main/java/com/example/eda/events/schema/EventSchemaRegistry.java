package com.example.eda.events.schema;

import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

/**
 * Loads JSON Schema definitions from classpath:schemas/ and validates event payloads.
 *
 * Schema files follow the naming convention: {event_type}-v{N}.json
 * Examples:
 *   schemas/example.created-v1.json
 *   schemas/example.created-v2.json
 *   schemas/example.updated-v1.json
 *
 * Validation behaviour:
 *   - If a schema file exists for event_type + schema_version → validate strictly
 *   - If no schema file registered for the combination → skip validation (opt-in)
 *
 * This allows incremental schema adoption: add a schema file only for the event types
 * where you want compile-time safety guarantees. Unregistered types are not validated.
 *
 * Called by EventPublisher implementations before publishing to the broker, so schema
 * violations are caught at the producer — not discovered by a distant consumer.
 */
@Component
public class EventSchemaRegistry {

    private static final Logger log = LoggerFactory.getLogger(EventSchemaRegistry.class);

    private final Map<String, JsonSchema> schemas = new HashMap<>();
    private final ObjectMapper objectMapper;

    public EventSchemaRegistry(ObjectMapper objectMapper) throws IOException {
        this.objectMapper = objectMapper;
        loadSchemas();
    }

    private void loadSchemas() throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("classpath:schemas/*.json");

        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);

        for (Resource resource : resources) {
            String filename = resource.getFilename();
            if (filename == null) continue;

            String base = filename.substring(0, filename.length() - 5); // strip ".json"
            int versionMarker = base.lastIndexOf("-v");
            if (versionMarker < 0) {
                log.warn("Skipping schema file with unexpected naming: {} (expected: event.type-v1.json)", filename);
                continue;
            }

            String eventType = base.substring(0, versionMarker);
            String version   = base.substring(versionMarker + 2);
            String key       = schemaKey(eventType, version);

            try {
                JsonSchema schema = factory.getSchema(resource.getInputStream());
                schemas.put(key, schema);
                log.info("Loaded event schema: eventType={} version={}", eventType, version);
            } catch (Exception ex) {
                log.error("Failed to load event schema file: {}", filename, ex);
            }
        }

        log.info("EventSchemaRegistry initialised with {} schema(s)", schemas.size());
    }

    /**
     * Validates the event payload against its registered schema.
     * No-op if no schema is registered for the event type + version combination.
     *
     * @throws SchemaValidationException if validation fails
     */
    public void validate(EventEnvelope envelope) {
        String key = schemaKey(envelope.eventType(), envelope.schemaVersion());
        JsonSchema schema = schemas.get(key);

        if (schema == null) {
            // No schema registered for this event type — skip validation
            return;
        }

        JsonNode payloadNode = objectMapper.valueToTree(envelope.payload());
        Set<ValidationMessage> errors = schema.validate(payloadNode);

        if (!errors.isEmpty()) {
            Set<String> messages = errors.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.toSet());
            throw new SchemaValidationException(envelope.eventType(), envelope.schemaVersion(), messages);
        }
    }

    /**
     * Returns true if a schema is registered for the given event type and version.
     */
    public boolean hasSchema(String eventType, String version) {
        return schemas.containsKey(schemaKey(eventType, version));
    }

    private static String schemaKey(String eventType, String version) {
        return eventType + "#" + version;
    }
}
