# Architecture Deep Dive

This document explains the full system architecture of `spring-eda-boilerplate` — how every layer is wired together, how a request flows from the client to the database and back, and how to verify each part is working locally.

For a quick pattern reference see [BOILERPLATE_SPEC.md](BOILERPLATE_SPEC.md). For getting started see [README.md](README.md).

---

## System overview

```
Browser / Client
      │
      │ HTTPS + JWT
      ▼
┌─────────────────────────────────────────────────────┐
│                    api-gateway :9080                 │
│  - Validates JWT signature against OIDC issuer       │
│  - Extracts tenant_id from JWT claim                 │
│  - Forwards X-Tenant-Id + X-Correlation-Id headers  │
│  - Rate limiting via Redis                           │
└──────────────┬───────────────────┬──────────────────┘
               │ HTTP              │ HTTP
               ▼                   ▼
   ┌───────────────────┐  ┌─────────────────────┐
   │  command-service  │  │   query-service      │
   │  :8081            │  │   :8082              │
   │                   │  │                      │
   │  POST /commands/* │  │  GET /queries/*      │
   │  PUT  /commands/* │  │                      │
   │  DELETE /commands │  │  Reads from          │
   │                   │  │  example_read_models │
   │  Writes to        │  │  (never touches      │
   │  example_entities │  │   write DB tables)   │
   └────────┬──────────┘  └──────────────────────┘
            │
            │ Same DB transaction
            ▼
   ┌───────────────────┐
   │  outbox_records   │  (PostgreSQL table)
   │  status=PENDING   │
   └────────┬──────────┘
            │ polls every 1s
            ▼
   ┌───────────────────┐
   │   outbox-relay    │
   │   :8084           │
   └────────┬──────────┘
            │ publishes event
            ▼
   ┌────────────────────────────────────────────────┐
   │                  Kafka topic                   │
   └──────────┬───────────────────┬─────────────────┘
              │                   │
              ▼                   ▼
   ┌──────────────────┐  ┌──────────────────────────┐
   │  query-service   │  │   consumer-service :8083  │
   │  ExampleProjector│  │   ExampleCreatedHandler   │
   │  → updates       │  │   → side effects          │
   │  read model      │  │   (notifications, etc)    │
   └──────────────────┘  └──────────────────────────┘
```

---

## Architectural patterns

### CQRS — Command Query Responsibility Segregation

CQRS covers exactly two services. Everything else is outside its scope.

| Service | CQRS role | What it owns |
|---|---|---|
| `command-service` | Write side | `example_entities` table, business rules, invariants |
| `query-service` | Read side | `example_read_models` table, projections, query endpoints |
| `consumer-service` | Not CQRS | Event-driven pub/sub — side effects like notifications |

The two sides never share a database connection. The read model is updated exclusively via events — the query service never reads from the write tables.

**The tradeoff:** after a write the read model is not immediately updated. The event must travel through the outbox relay and broker first — typically milliseconds locally. Clients must tolerate this brief window of eventual consistency.

---

### Transactional Outbox

The outbox pattern guarantees an event is never lost even if the broker is down.

```
Command handler — single @Transactional boundary:

  BEGIN
    INSERT INTO example_entities (...)     ← domain write
    INSERT INTO outbox_records (           ← event write
      event_type = 'example.created',
      payload    = '{...}',
      status     = 'PENDING'
    )
  COMMIT

  If the broker is down: transaction still commits.
  The outbox record stays PENDING until the relay can publish it.
  No event is ever silently dropped.
```

`outbox-relay` runs as a separate service, polling `outbox_records WHERE status = 'PENDING'` every second. For each record it:
1. Builds an `EventEnvelope`
2. Publishes to the broker
3. Marks the record `PUBLISHED`

If publishing fails, the record stays `PENDING` and is retried on the next poll.

---

### Idempotent Consumer

Kafka delivers events **at least once** — under failure conditions the same event may arrive twice. The idempotent consumer pattern makes duplicate delivery harmless.

```
Consumer receives event:

  if (inboxDeduplicator.isDuplicate(eventId)) {
      return;  // already processed — skip silently
  }

  // process the event
  doBusinessLogic();

  inboxDeduplicator.markProcessed(eventId);  // same transaction as business logic
```

`inbox_records` has a unique constraint on `event_id`. If the same event arrives twice, the second `markProcessed` call throws a constraint violation — the transaction rolls back, and the event is skipped cleanly.

---

### Event Choreography

Choreography is how services react to each other without a central coordinator. Each service listens for events it cares about and publishes new events as a result.

```
command-service publishes "example.created"
    │
    ├─ query-service reacts      → updates read model → (no further event)
    │
    ├─ consumer-service reacts   → sends notification → (no further event)
    │
    └─ command-service reacts    → activates entity   → publishes "example.activated"
           (ExampleChoreographyHandler)                       │
                                                              ▼
                                                    any subscriber can react
```

No service knows what other services exist — they only know about event types.

**Choreography vs Orchestration:**

| | Choreography (this boilerplate) | Orchestration |
|---|---|---|
| Coordinator | None — services react independently | Central saga class tracks each step |
| State tracking | No saga state needed | Requires `saga_instances` DB table |
| Coupling | Services only know about events | Orchestrator knows all participants |
| Best for | Linear flows, loose coupling | Complex flows with branching, retries, compensation |
| Frameworks | None needed | Axon, Conductor, Temporal |

Use choreography by default. Upgrade to orchestration when flows branch significantly, compensation needs to be tracked across many services, or you need to query "what step is this saga on right now."

---

### Multi-Tenancy — PostgreSQL Row-Level Security

Every tenant shares the same database. RLS ensures each tenant sees only their own rows — enforced inside the database, not in application code.

**How it works:**

```sql
-- Policy on every tenant-scoped table
CREATE POLICY tenant_isolation ON example_entities
    USING (tenant_id = current_setting('app.tenant_id', true));
```

Before every database call, `RlsDataSourceInterceptor` sets the session variable:

```sql
SET LOCAL app.tenant_id = 'tenant-1';
```

PostgreSQL then silently rewrites every query:

```sql
-- Application writes:
SELECT * FROM example_entities;

-- PostgreSQL executes:
SELECT * FROM example_entities WHERE tenant_id = 'tenant-1';
```

**Safe default:** `current_setting('app.tenant_id', true)` returns `null` if the setting is missing (the `true` argument suppresses the error). Since `tenant_id = null` is always false in PostgreSQL, a missing tenant context returns zero rows — never all rows.

**`SET LOCAL` scope:** the variable is scoped to the current transaction only. It resets automatically at transaction end — no risk of one request's tenant context leaking into the next.

**Database roles:**

| Role | RLS | Used by |
|---|---|---|
| `app_user` | Enforced — filtered by tenant | All application services |
| `platform_operator` | Bypassed | Flyway migrations, admin tools |

---

## Request flow — end to end

### POST /api/commands/examples

```
1. Client sends JWT in Authorization header

2. api-gateway (:9080)
   - Fetches JWKS from OIDC issuer (mock-oidc locally)
   - Validates JWT signature and expiry
   - Extracts tenant_id claim → adds X-Tenant-Id header
   - Generates or forwards X-Correlation-Id header
   - Routes to command-service:8081/commands/examples

3. command-service — MdcFilter (shared-logging)
   - Reads X-Correlation-Id, writes to MDC
   - Every log line from this point carries correlation_id

4. command-service — TenantContextFilter (shared-security)
   - Reads X-Tenant-Id header
   - Stores in TenantContextHolder (ThreadLocal)
   - Writes tenant_id to MDC

5. command-service — RlsDataSourceInterceptor (shared-db)
   - Intercepts every Spring Data repository call
   - Executes: SET LOCAL app.tenant_id = 'tenant-1'
   - PostgreSQL RLS now filters all queries for this tenant

6. command-service — ExampleCommandHandler (@Transactional)
   BEGIN
     INSERT INTO example_entities
       (tenant_id='tenant-1', name='hello', status='CREATED')
     INSERT INTO outbox_records
       (event_type='example.created', status='PENDING', payload={id, name, tenantId})
   COMMIT
   → Returns 201 {"id": "uuid"}

7. outbox-relay — OutboxRelayPoller (polls every 1s)
   - SELECT * FROM outbox_records WHERE status='PENDING' LIMIT 10
   - Builds EventEnvelope:
       {
         event_id:       uuid,
         event_type:     "example.created",
         tenant_id:      "tenant-1",
         correlation_id: uuid,
         occurred_at:    timestamp,
         payload:        {id, name, tenantId}
       }
   - Publishes to Kafka
   - UPDATE outbox_records SET status='PUBLISHED'

8. Kafka delivers to all consumer groups simultaneously:

   ┌─ query-service — ExampleProjector
   │    - supports("example.created") → true
   │    - UPSERT into example_read_models
   │    - GET /queries/examples now returns this entity
   │
   ├─ command-service — ExampleChoreographyHandler
   │    - supports("example.created") → true
   │    - isDuplicate(eventId) → false (first delivery)
   │    - entity.activate() → status = ACTIVE
   │    - INSERT into outbox_records (event_type='example.activated')
   │    - markProcessed(eventId)
   │    - outbox-relay picks up → publishes "example.activated"
   │
   └─ consumer-service — ExampleCreatedHandler
        - supports("example.created") → true
        - isDuplicate(eventId) → false
        - [your side effect logic here]
        - markProcessed(eventId)
```

### GET /api/queries/examples

```
1. Gateway validates JWT, routes to query-service:8082

2. TenantContextFilter sets tenant_id in ThreadLocal

3. RlsDataSourceInterceptor: SET LOCAL app.tenant_id = 'tenant-1'

4. ExampleQueryController
   - repository.findAll()
   - PostgreSQL RLS filters to tenant-1 rows only
   - Returns list from example_read_models
```

---

## Tenant isolation — full chain

```
JWT: { tenant_id: "tenant-1", roles: ["TENANT_ADMIN"] }
  │
  ▼
TenantContextFilter
  → TenantContextHolder.set(new TenantContext("tenant-1", subject, roles))
  → MDC.put("tenant_id", "tenant-1")
  │
  ▼
RlsDataSourceInterceptor (AOP — fires before every @Repository method)
  → connection.execute("SET LOCAL app.tenant_id = 'tenant-1'")
  │
  ▼
PostgreSQL RLS policy
  → WHERE tenant_id = current_setting('app.tenant_id', true)
  → WHERE tenant_id = 'tenant-1'

Tenant-1 data returned. Tenant-2 data invisible.

If tenant_id is missing at any layer → zero rows returned, never all rows.
```

---

## Event envelope

Every event travelling through the system uses the same envelope:

```json
{
  "event_id":       "550e8400-e29b-41d4-a716-446655440000",
  "event_type":     "example.created",
  "tenant_id":      "tenant-1",
  "correlation_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "causation_id":   "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
  "occurred_at":    "2025-01-15T10:30:00Z",
  "payload": {
    "id":       "uuid",
    "name":     "hello",
    "tenantId": "tenant-1"
  }
}
```

| Field | Purpose |
|---|---|
| `event_id` | Unique event ID — used by `InboxDeduplicator` to detect duplicates |
| `event_type` | `noun.verb` format — used by `EventConsumer.supports()` to route events |
| `tenant_id` | Carried in every event so consumers can set RLS context without a JWT |
| `correlation_id` | Same across all events in a single user-initiated flow — links a chain of events |
| `causation_id` | The `event_id` of the event that caused this one — links parent → child |
| `occurred_at` | When the event happened on the producer side |
| `payload` | Event-Carried State Transfer — everything consumers need, no callbacks required |

---

## Observability wiring

Every log line from every service carries the same fields:

```json
{
  "timestamp":      "2025-01-15T10:30:00.123Z",
  "level":          "INFO",
  "service":        "example-command-service",
  "tenant_id":      "tenant-1",
  "correlation_id": "uuid",
  "trace_id":       "abc123def456",
  "span_id":        "789ghi",
  "message":        "Entity created id=uuid"
}
```

`trace_id` is injected into MDC by `TracingMdcSpanHandler` at span start — identical across all services for a single request. This enables trace-to-log correlation: find a trace in Tempo, click through to the exact log lines in Loki.

```
Grafana
  ├─ Tempo      ← distributed traces via OTel Java agent
  ├─ Loki       ← structured JSON logs via loki-logback-appender
  └─ Prometheus ← metrics via Micrometer /actuator/prometheus (scraped every 15s)
```

**Key metrics exposed:**

| Metric | What it shows |
|---|---|
| `http_server_requests_seconds` | Request rate, latency, error rate per endpoint |
| `outbox_records_pending_total` | Backlog of unpublished events — alert if this grows |
| `jvm_threads_total` | Virtual thread count |
| `jvm_memory_used_bytes` | Heap usage |

---

## Circuit Breaker

The circuit breaker prevents cascading failures — when a downstream service is unhealthy, requests fail immediately instead of piling up and exhausting threads.

### Where it applies

```
PROTECTED BY CIRCUIT BREAKER:        NOT NEEDED:
  api-gateway → command-service         Kafka publishing (outbox handles it)
  api-gateway → query-service           DB writes (transactions handle it)
  command-service → Stripe API          Event consumption (retry handles it)
```

### Three states

```
CLOSED (normal)                OPEN (tripped)              HALF-OPEN (recovery check)
  │                              │                            │
  requests flow through          requests fail immediately    one test request allowed
  failures counted               no downstream call made      success → CLOSED
  threshold crossed ──────────►  wait 30s ──────────────────► fail → OPEN again
```

### api-gateway — route-level circuit breakers

Each gateway route has its own circuit breaker. When `command-service` is down:

```
Client POST /api/commands/examples
  │
  ▼
api-gateway — circuit breaker: command-service (OPEN)
  │
  ▼  fail fast — no connection attempt to command-service
FallbackController.commandServiceFallback()
  │
  ▼
HTTP 503 + Retry-After: 30
  {
    "error": "service_unavailable",
    "message": "Command service is temporarily unavailable. Please retry in 30 seconds.",
    "retryAfterSeconds": "30"
  }
```

### Stripe — method-level circuit breaker

Every `PaymentGateway` method is wrapped:

```java
return circuitBreaker.executeSupplier(() -> callStripe(request));
// If OPEN → throws PaymentException("Payment service temporarily unavailable")
// No Stripe API call is made — no timeout, no thread blocking
```

### Configuration

`shared-resilience/resilience-defaults.yml` defines two base configs:

| Config | Used for | Failure threshold | Open wait |
|---|---|---|---|
| `default` | Internal service calls | 50% of last 10 calls | 30s |
| `external-api` | Stripe | 40% of last 10 calls | 60s |

Override per instance in each service's `application.yml`.

### Metrics in Grafana

Resilience4j publishes to Micrometer automatically:

```
# Circuit breaker state: 0=CLOSED, 1=OPEN, 2=HALF_OPEN
resilience4j_circuitbreaker_state{name="stripe"}

# Current failure rate percentage
resilience4j_circuitbreaker_failure_rate{name="command-service"}

# Call counts by outcome
resilience4j_circuitbreaker_calls_total{name="stripe", kind="successful|failed|not_permitted"}
```

Alert to set: `resilience4j_circuitbreaker_state == 1` → P2 alert, circuit breaker opened.

---

## Shared library responsibilities

Each shared library is a Spring Boot auto-configuration module — services include it as a dependency and it wires itself automatically via `AutoConfiguration.imports`.

| Library | Auto-wires | Key classes |
|---|---|---|
| `shared-logging` | MDC filter, Logback JSON + Loki config | `MdcFilter`, `MdcTaskDecorator` |
| `shared-telemetry` | OTel span → MDC bridge, virtual thread metrics | `TracingMdcSpanHandler`, `VirtualThreadMetrics` |
| `shared-security` | JWT resource server, tenant context filter | `TenantContextFilter`, `TenantContextHolder`, `SecurityAutoConfiguration` |
| `shared-db` | RLS interceptor, outbox writer, inbox deduplicator | `RlsDataSourceInterceptor`, `OutboxWriter`, `InboxDeduplicator` |
| `shared-events` | Event publisher (Kafka or SNS/SQS), no-op fallback | `KafkaEventPublisher`, `SnsEventPublisher`, `NoOpEventPublisher` |
| `shared-payments` | Stripe payment gateway, webhook verifier | `StripePaymentGateway`, `StripeWebhookVerifier` |

---

## Local verification guide

### Start the stack

```bash
cp .env.example .env
make infra                      # postgres, redis, kafka, mock-oidc, observability
./gradlew build -x test         # compile all modules
docker compose build            # build service images
make up                         # start all services
make logs                       # wait until all services print "Started ... in X seconds"
```

### Get a token

```bash
TOKEN=$(curl -s -X POST http://localhost:8090/okta/token \
  -d "grant_type=client_credentials&client_id=test&client_secret=test&scope=openid" \
  | jq -r '.access_token')

echo $TOKEN    # should be a JWT string, not null
```

### Verify the full happy path

```bash
# 1. Create an entity
RESPONSE=$(curl -s -X POST http://localhost:9080/api/commands/examples \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "hello"}')
echo $RESPONSE              # {"id": "some-uuid"}
ID=$(echo $RESPONSE | jq -r '.id')

# 2. Wait for outbox relay + projection + choreography
sleep 2

# 3. Query the read model
curl -s http://localhost:9080/api/queries/examples \
  -H "Authorization: Bearer $TOKEN" | jq
# Should return the entity — status=ACTIVE (choreography handler activated it)
```

### Verify the database directly

```bash
docker exec -it $(docker ps -qf name=postgres) psql -U postgres -d edadb
```

```sql
-- Write model
SELECT id, name, status, tenant_id FROM example_entities;

-- Read model (populated by query-service projection)
SELECT id, name, tenant_id FROM example_read_models;

-- Outbox — should be PUBLISHED
SELECT event_type, status, created_at FROM outbox_records ORDER BY created_at DESC LIMIT 10;

-- Inbox — deduplication records from consumer-service and choreography handler
SELECT event_id, processed_at FROM inbox_records ORDER BY processed_at DESC LIMIT 10;

\q
```

### Verify RLS tenant isolation

```sql
-- Connect as app_user with no tenant context set
-- Should return zero rows even though data exists
SET app.tenant_id = '';
SELECT * FROM example_entities;   -- 0 rows

-- Set the correct tenant
SET app.tenant_id = 'tenant-1';
SELECT * FROM example_entities;   -- rows visible
```

### Verify security

```bash
# Without token → 401
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:9080/api/commands/examples \
  -H "Content-Type: application/json" -d '{"name":"x"}'
# 401

# Actuator health is public — no token needed
curl -s http://localhost:8081/actuator/health | jq .status   # "UP"
curl -s http://localhost:8082/actuator/health | jq .status   # "UP"
curl -s http://localhost:8083/actuator/health | jq .status   # "UP"
curl -s http://localhost:8084/actuator/health | jq .status   # "UP"
```

### Verify observability

Open **Grafana at http://localhost:3000** (admin / admin):

| What to check | Where |
|---|---|
| Service health dashboard | Dashboards → Service Health |
| Structured logs with tenant_id | Explore → Loki → `{service="example-command-service"}` |
| Distributed trace across services | Explore → Tempo → paste a `trace_id` from a log line |
| Request rate and latency | Explore → Prometheus → `http_server_requests_seconds_count` |
| Outbox backlog | Explore → Prometheus → `outbox_records_pending_total` |

---

## What each verification proves

| Test | What it proves |
|---|---|
| POST → 201 | Command handler, RLS write, outbox write |
| GET returns entity | Outbox relay, Kafka, projector, read model |
| Status = ACTIVE | Choreography handler received and processed the event |
| Outbox status = PUBLISHED | Relay polled and published successfully |
| Inbox records exist | Idempotent consumer deduplication is running |
| DB query without tenant → 0 rows | RLS isolation enforced at database level |
| 401 without token | Gateway JWT validation |
| Actuator health returns UP | All services healthy |
| Grafana trace links to logs | OTel + Loki + Tempo wired correctly |
