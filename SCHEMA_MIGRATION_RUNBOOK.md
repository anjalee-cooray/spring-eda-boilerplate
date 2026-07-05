# Schema Migration Runbook — Blue/Green Deployment

This runbook describes how to evolve a domain event schema without downtime. Follow
each phase in order. Never skip a phase, even for "simple" field additions.

---

## Overview

Events in the outbox and event_store are immutable once written. A consumer that reads
an old event at runtime must be able to understand it. The upcaster chain
(`EventUpcasterRegistry`) handles this: it upgrades an event from its stored version to
the version the current handler expects, one step at a time.

The golden rule: **both the old schema version and the new schema version must be
readable by every live service before the old version is retired.**

---

## Phase 1 — Write the upcaster BEFORE changing any producer

1. Create a migration directory for the new schema version:
   ```bash
   ./scripts/new-schema.sh --event-type example.created --version 2 --service example-consumer-service
   ```
   This scaffolds:
   - `schemas/example.created/v2.json` — JSON Schema for v2
   - `ExampleCreatedV1ToV2Upcaster.java` — stub upcaster in the consumer service

2. Implement the upcaster:
   ```java
   @Component
   public class ExampleCreatedV1ToV2Upcaster implements EventUpcaster {
       @Override public String eventType()   { return "example.created"; }
       @Override public String fromVersion() { return "1"; }
       @Override public String toVersion()   { return "2"; }

       @Override
       public EventEnvelope upcast(EventEnvelope envelope) {
           Map<String, Object> payload = new java.util.LinkedHashMap<>((Map<?,?>) envelope.payload());
           // backfill the new field with a safe default
           payload.putIfAbsent("newRequiredField", "default-value");
           return new EventEnvelope(
                   envelope.eventId(), envelope.eventType(), envelope.tenantId(),
                   envelope.correlationId(), envelope.causationId(), envelope.occurredAt(),
                   payload, "2", envelope.aggregateId());
       }
   }
   ```

3. Deploy the consumer service with the new upcaster. The consumer now handles
   **both v1 and v2** events. The producer still emits v1.

4. Verify: `SchemaCompatibilityValidator` logs `chain=[v1→v2]` at startup with no errors.

---

## Phase 2 — Deploy the producer update (blue/green)

1. Update the producer to emit v2 events (bump `schemaVersion` in `EventEnvelope`).

2. Deploy using blue/green:
   - Bring up the green (new) producer pods alongside blue (old).
   - Route a small percentage of traffic to green. Monitor DLQ and consumer logs.
   - Gradually shift 100% traffic to green.
   - Terminate blue pods.

3. During the overlap period the broker will contain a mix of v1 and v2 events.
   The consumer's upcaster handles both transparently.

---

## Phase 3 — Retire the old schema version (optional)

Once the event_store contains no v1 events within the replay window:

1. Check for stale v1 events:
   ```sql
   SELECT COUNT(*) FROM event_store
   WHERE event_type = 'example.created' AND schema_version = '1';
   ```

2. If count = 0, the upcaster may be removed in a subsequent release.
   Keep the upcaster for at least one full replay retention period (default 365 days)
   before removing it — archived events may still arrive from event_store_archive.

3. Update `schemas/example.created/v1.json` with a deprecation comment.

---

## Chain contiguity rule

`SchemaCompatibilityValidator` runs at startup and will **throw** if any upcaster chain
has a gap. Example of a bad state:

| Upcaster | State |
|---|---|
| v1 → v2 | ✅ registered |
| v3 → v4 | ✅ registered |
| v2 → v3 | ❌ MISSING |

A v1 event would be upcasted to v2 and then stop — the v3/v4 handler never sees it.
The service will refuse to start. Register the missing upcaster before deploying.

---

## Rollback procedure

If the green producer must be rolled back before blue is retired:

1. Roll back green pods to the previous image.
2. Both v1 and v2 events may still be in the broker from the partial rollout.
3. The v1→v2 upcaster is still in the consumer — no consumer rollback needed.
4. Investigate the root cause. Re-attempt Phase 2 once fixed.

---

## Adding a breaking change (field removal or rename)

Breaking changes require a new major event type name, not a version bump:

```
example.created      — v1 and v2 (additive changes)
example.created.v3   — new type for breaking change
```

Consumers subscribe to both event types and handle both in parallel. Producers emit
the new type only. Old type is retired after the retention window.

---

## Verification checklist before going to production

- [ ] `SchemaCompatibilityValidator` logs `all upcaster chains contiguous ✓` in staging
- [ ] DLQ count is 0 for 30 minutes after green deployment
- [ ] Consumer lag metric `kafka.consumer.lag` is within SLO
- [ ] `event_store` contains records at the new schema version
- [ ] Replay from event_store produces correct read-model data at new version
