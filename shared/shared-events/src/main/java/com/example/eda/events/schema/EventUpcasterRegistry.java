package com.example.eda.events.schema;

import com.example.eda.events.envelope.EventEnvelope;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Applies the EventUpcaster chain to an incoming event before handler dispatch.
 *
 * Consumers call upcastToLatest(envelope) once. If the event's schema_version is
 * older than the current version, every registered upcaster for that event type is
 * applied in version order until the event reaches the latest known version.
 *
 * If the event is already at the latest version (or no upcasters are registered),
 * the original envelope is returned unchanged — zero allocation overhead on the
 * happy path.
 *
 * Chain example: v1 → v2 → v3 (two upcasters run in sequence)
 */
@Component
public class EventUpcasterRegistry {

    private static final Logger log = LoggerFactory.getLogger(EventUpcasterRegistry.class);

    // key: "eventType#fromVersion" → upcaster
    private final Map<String, EventUpcaster> upcasters;

    public EventUpcasterRegistry(List<EventUpcaster> upcasters) {
        this.upcasters = new HashMap<>();
        for (EventUpcaster upcaster : upcasters) {
            String key = chainKey(upcaster.eventType(), upcaster.fromVersion());
            this.upcasters.put(key, upcaster);
            log.info("Registered upcaster: eventType={} {}→{}",
                    upcaster.eventType(), upcaster.fromVersion(), upcaster.toVersion());
        }
    }

    /**
     * Applies upcasters in chain order until no further upcaster matches.
     * Returns the (possibly upgraded) envelope ready for handler dispatch.
     */
    public EventEnvelope upcastToLatest(EventEnvelope envelope) {
        EventEnvelope current = envelope;

        while (true) {
            String key = chainKey(current.eventType(), current.schemaVersion());
            EventUpcaster upcaster = upcasters.get(key);

            if (upcaster == null) {
                break; // already at latest version, or no upcaster registered
            }

            log.debug("Upcasting event eventId={} eventType={} {}→{}",
                    current.eventId(), current.eventType(),
                    upcaster.fromVersion(), upcaster.toVersion());

            current = upcaster.upcast(current);
        }

        return current;
    }

    /**
     * Returns all registered upcasters grouped by event type.
     * Used by SchemaCompatibilityValidator to check chain contiguity on startup.
     */
    public Map<String, List<EventUpcaster>> getAllUpcasters() {
        Map<String, List<EventUpcaster>> grouped = new HashMap<>();
        for (EventUpcaster u : upcasters.values()) {
            grouped.computeIfAbsent(u.eventType(), k -> new java.util.ArrayList<>()).add(u);
        }
        return grouped;
    }

    private static String chainKey(String eventType, String version) {
        return eventType + "#" + version;
    }
}
