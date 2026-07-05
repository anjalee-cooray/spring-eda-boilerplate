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
┌─────────────────────────────────────────────────────────────────┐
│                       api-gateway :9080                          │
│  - Validates JWT signature against OIDC issuer                   │
│  - Extracts tenant_id from JWT claim                             │
│  - Forwards X-Tenant-Id + X-Correlation-Id headers              │
│  - Rate limiting (Redis token bucket, per tenant)               │
│  - HTTP idempotency (Redis dedup, Idempotency-Key header)        │
│  - Circuit breaker + retry on downstream routes                  │
└──────────────┬───────────────────────┬──────────────────────────┘
               │ HTTP                  │ HTTP
               ▼                       ▼
   ┌───────────────────┐    ┌─────────────────────┐
   │  command-service  │    │   query-service      │
   │  :8081            │    │   :8082              │
   │  CQRS write side  │    │   CQRS read side     │
   │                   │    │                      │
   │  POST /commands/* │    │  GET /queries/*      │
   │  PUT  /commands/* │    │                      │
   │  DELETE /commands │    │  Reads from          │
   │                   │    │  example_read_models │
   │  Writes to        │    │  (never touches      │
   │  example_entities │    │   write DB tables)   │
   └────────┬──────────┘    └──────────────────────┘
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
            │ publishes via EventPublisher
            ▼
   ┌──────────────────────────────────────────────────────────────┐
   │            Message broker (pluggable — one env var)          │
   │                                                              │
   │  EVENTS_BROKER=kafka (default, local dev)                    │
   │    Kafka topic per event type                                │
   │    DLQ: {topic}.dlq topic                                    │
   │    Retry: DefaultErrorHandler (3×, exponential backoff)      │
   │                                                              │
   │  EVENTS_BROKER=sns (AWS production)                          │
   │    SNS topic → SQS queue per consumer service               │
   │    DLQ: SQS Redrive Policy → separate DLQ queue             │
   │    Retry: SQS re-delivery + changeMessageVisibility backoff  │
   └──────────┬──────────────────────────┬───────────────────────┘
              │                          │
              ▼                          ▼
   ┌──────────────────┐      ┌──────────────────────────┐
   │  query-service   │      │   consumer-service :8083  │
   │  ExampleProjector│      │   ExampleCreatedHandler   │
   │  → updates       │      │   → side effects          │
   │  read model      │      │   (notifications, etc)    │
   │                  │      │                           │
   │                  │      │  DlqConsumer / SqsDlqConsumer
   │                  │      │  → logs + metrics on DLQ  │
   └──────────────────┘      └──────────────────────────┘
```

### Broker selection

Switch the entire messaging layer with one environment variable — no code changes:

| `EVENTS_BROKER` | Publisher | Consumer | DLQ mechanism | Local dev |
|---|---|---|---|---|
| `kafka` (default) | `KafkaEventPublisher` | `KafkaEventConsumer` + `DefaultErrorHandler` | `{topic}.dlq` Kafka topic | Kafka in Docker Compose |
| `sns` | `SnsEventPublisher` | `SqsEventConsumer` + `changeMessageVisibility` | SQS Redrive Policy | LocalStack (`make infra-sns`) |

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

### Dead Letter Queue (DLQ)

Kafka delivers events at least once, but some messages will fail processing permanently — a malformed payload, a missing dependency, or a code bug. Without a DLQ, the consumer would either:
- Block forever on the poison message (auto-offset-commit disabled)
- Skip it silently and lose the event

The DLQ pattern gives a third option: **retry a fixed number of times, then park the message for operator inspection**.

#### Retry policy

`KafkaConsumerConfig` (in `shared-events`) registers a `DefaultErrorHandler` on the `ConcurrentKafkaListenerContainerFactory`:

```
Attempt 1  →  fails  →  wait 1s
Attempt 2  →  fails  →  wait 2s
Attempt 3  →  fails  →  wait 4s
After attempt 3 → route to {original-topic}.dlq
```

Total elapsed before DLQ: ~7 seconds. Non-retryable exceptions (`IllegalArgumentException`, `IllegalStateException`) skip retries and go directly to the DLQ — retrying a malformed payload will never succeed.

#### DLQ topic naming

```
example.created  →  example.created.dlq
```

Every topic the service consumes has a paired DLQ topic. The `DeadLetterPublishingRecoverer` routes to `{original-topic}.dlq` automatically. Kafka auto-creates the DLQ topic on first write (in local dev); in production, pre-create it with the same partition count as the source topic.

#### DLQ message headers

Spring Kafka's `DeadLetterPublishingRecoverer` adds these headers to every DLQ message:

| Header | Content |
|---|---|
| `kafka_dlt-original-topic` | Source topic name |
| `kafka_dlt-original-partition` | Source partition number |
| `kafka_dlt-original-offset` | Source message offset |
| `kafka_dlt-exception-message` | Exception message |
| `kafka_dlt-exception-fqcn` | Exception class name |

These headers are logged by `DlqConsumer` and are the primary tool for operator triage.

#### DlqConsumer

`DlqConsumer` in `example-consumer-service` listens on `app.events.kafka.dlq-topics` with a separate consumer group (`{service-name}-dlq`):

```
DLQ message arrives
  → log at ERROR with all DLT headers + payload
  → increment events.dlq.received{topic=example.created.dlq}
  → return (do NOT re-throw — re-throwing loops back to DLQ)
```

The Micrometer counter drives a Prometheus alert:

```promql
rate(events_dlq_received_total[5m]) > 0
```

Alert severity: **P1** — every DLQ message means a business event was not processed.

#### Operator runbook

1. Check service logs for `DLQ event received` — `sourceTopic`, `sourcePartition`, `sourceOffset`, and `exceptionMessage` identify the exact failure
2. Inspect the payload in the log to determine whether it is a code bug or bad data

**Code bug** → fix the handler, redeploy, then replay the DLQ:
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group example-consumer-service-dlq \
  --topic example.created.dlq \
  --reset-offsets --to-earliest --execute
```

**Bad data (poison message)** → advance past it:
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group example-consumer-service-dlq \
  --topic example.created.dlq \
  --reset-offsets --to-latest --execute
```

#### DLQ — Kafka vs SNS/SQS comparison

| Concern | Kafka | SNS/SQS |
|---|---|---|
| Who retries | Application (`DefaultErrorHandler`, 3×, exponential backoff) | AWS (re-delivers after `visibilityTimeout`; app extends timeout for backoff) |
| Who routes to DLQ | Application (`DeadLetterPublishingRecoverer`) | AWS (Redrive Policy after `maxReceiveCount`) |
| DLQ target | Kafka topic (`example.created.dlq`) | SQS queue (`example-created-dlq`) |
| DLQ consumer | `DlqConsumer` — `@KafkaListener` | `SqsDlqConsumer` — `@Scheduled` poller |
| Alert | Prometheus `events.dlq.received` counter | CloudWatch `ApproximateNumberOfMessagesVisible` + Prometheus counter |
| Replay | `kafka-consumer-groups --reset-offsets` | AWS Console → "Start DLQ redrive" |
| Config lives in | Java (`KafkaConsumerConfig`) | AWS infrastructure (Terraform Redrive Policy) |

---

### SNS/SQS DLQ handling

When `EVENTS_BROKER=sns`, DLQ routing is handled at the AWS infrastructure level via the **Redrive Policy** configured on the source SQS queue:

```
Publisher (outbox-relay)
  → SNS topic (example-created)
    → SQS queue (example-created) ─── subscribed
          │
          │  consumer receives message
          │  fails → does NOT delete → visibility timeout expires → re-delivered
          │  fails again (maxReceiveCount times)
          ▼
    SQS DLQ (example-created-dlq)  ← AWS moves it automatically
          │
          │  SqsDlqConsumer polls every 5s
          ▼
    log at ERROR + increment events.dlq.received{queue=example-created-dlq}
    delete from DLQ after logging
```

**Exponential backoff (application-level):**

`SqsEventConsumer` calls `changeMessageVisibility` on failure to space out redeliveries:

| Receive count | Visibility extension |
|---|---|
| 1st failure | 10 seconds |
| 2nd failure | 30 seconds |
| 3rd+ failure | 90 seconds |

After `maxReceiveCount` (set to 3 on the queue Redrive Policy), AWS routes to DLQ regardless of visibility timeout.

**LocalStack setup for local dev:**

```bash
# Start LocalStack
make infra-sns

# Create topics, queues, DLQs, and subscriptions
make sns-setup

# Switch a service to SNS/SQS — set in environment or .env
EVENTS_BROKER=sns

# Uncomment the sqs/sns blocks in the service's application.yml
# and set endpoint-override: http://localhost:4566
```

**CloudWatch alarm (production):**

```hcl
resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "example-created-dlq-depth"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = "example-created-dlq" }
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  period              = 60
  statistic           = "Sum"
}
```

**Operator runbook (SNS/SQS):**

1. `SqsDlqConsumer` logs `DLQ message received` with `messageId`, `dlqQueue`, `receiveCount`, and full body
2. Find the original handler error by searching logs for that `messageId`
3. Code bug → fix the handler, redeploy, then replay via AWS Console: SQS → select DLQ → **Start DLQ redrive** → redrive to source queue
4. Bad data → message is already deleted from DLQ by `SqsDlqConsumer` — no further action

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

### Idempotency — Safe Retries on Commands

Implemented in `IdempotencyFilter` (api-gateway). Prevents duplicate processing when a client retries a command that already succeeded but whose response was lost in transit.

**Why the gateway, not the command service:**
One implementation in the gateway covers all current and future command services. The gateway already has Redis wired for rate limiting — idempotency caching uses the same instance.

**Flow:**

```
Client generates one UUID per user action (not per retry attempt)

POST /api/commands/appointments
  Idempotency-Key: 7c9e6679-7425-40de-944b-e07fc1f90ae7

IdempotencyFilter:
  cacheKey = "idempotency:tenant-1:7c9e6679-..."

  Redis HIT?
    → return cached {statusCode, body} immediately
    → response header: Idempotency-Status: HIT
    → command-service never called

  Redis MISS?
    → SETNX "idempotency:tenant-1:...:processing" (30s TTL)
       blocks a second concurrent request with the same key
    → forward to command-service
    → on response: cache {statusCode, body} in Redis (24h TTL)
    → delete processing marker
    → return response to client
    → response header: Idempotency-Status: MISS

  Concurrent duplicate (processing marker exists)?
    → return 409 Conflict immediately
    → client should wait briefly and retry
```

**Cache key scope:**

```
"idempotency:{tenant_id}:{idempotency_key}"
```

Scoped by `tenant_id` so keys from different tenants never collide. `tenant_id` is read from the `X-Tenant-Id` header already set by `TenantContextGatewayFilter`.

**What is and is not cached:**

| Response | Cached? | Reason |
|---|---|---|
| 2xx | Yes | Command succeeded — safe to replay |
| 4xx | Yes | Client error — same error on retry is correct |
| 5xx | No | Server error — command may not have run; allow retry to reach server |

**Retry contract for clients:**

```
1. Generate UUID once per user action
2. Send with every attempt: Idempotency-Key: <uuid>
3. On 503 (circuit open) or timeout → retry with SAME uuid
4. On 409 (concurrent duplicate) → wait 1s → retry with SAME uuid
5. On 2xx or 4xx → stop retrying
```

**Missing header policy:**
`POST`/`PUT`/`PATCH`/`DELETE` without `Idempotency-Key` → `400 Bad Request`.
`GET` requests skip the filter entirely — they are inherently idempotent.

**Retry on query routes (GET only):**
Query routes have a `Retry` filter (3 attempts, exponential backoff 500ms → 1s → 2s) because GET is safe to retry at any layer. Command routes intentionally have no gateway-level retry — the client retries with the idempotency key instead.

---

### Circuit Breaker + Retry — Resilience

Implemented in `shared-resilience`, applied to all synchronous calls that can fail or hang. Async event flows (outbox → Kafka) do not need these patterns — the outbox handles broker failures and Kafka handles consumer failures.

**Where applied:**

| Call | Circuit Breaker | Retry |
|---|---|---|
| `api-gateway → command-service` | `command-service` instance | `command-service` instance |
| `api-gateway → query-service` | `query-service` instance | `query-service` instance |
| `StripePaymentGateway → Stripe API` | `stripe` instance (external-api config) | `stripe` instance (external-api config) |

**Decorator order — circuit breaker outside, retry inside:**

```
Request → CircuitBreaker → Retry → actual call
                              ↑
                   retries happen here (max 2 for Stripe, 3 for internal)
          ↑
          sees final outcome after all retries exhausted
          only counts as a failure if all retries failed
```

This order is critical. If retry were outside the circuit breaker, each retry attempt would count as a separate call to the circuit breaker, tripping it on the first transient failure instead of after genuine sustained failure.

**What triggers retry vs what is ignored:**

```
Retried:   IOException, ConnectException, TimeoutException  ← network problems
Ignored:   PaymentException, IllegalArgumentException       ← business errors
```

A declined card wrapped in `PaymentException` is not retried — it will not succeed on a second attempt. Only genuine network failures are worth retrying.

**Exponential backoff:**

```
Attempt 1: fails → wait 500ms
Attempt 2: fails → wait 1000ms
Attempt 3: fails → circuit breaker records final failure
```

Backoff gives the downstream service time to recover between attempts and avoids hammering an already struggling service.

**When the circuit breaker opens:**

```
api-gateway routes → FallbackController → 503 + Retry-After header
StripePaymentGateway → throws PaymentException("temporarily unavailable")
```

**Metrics exposed (Prometheus / Grafana):**

```
resilience4j_circuitbreaker_state{name="stripe"}           → 0=closed, 1=open, 2=half-open
resilience4j_circuitbreaker_failure_rate{name="stripe"}    → current failure %
resilience4j_retry_calls_total{name="stripe", kind="successful_with_retry|failed_with_retry|..."}
```

Alert condition: `resilience4j_circuitbreaker_state == 1` — circuit breaker opened.

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
   - Publishes via EventPublisher (broker-agnostic):
       EVENTS_BROKER=kafka → KafkaEventPublisher → Kafka topic "example.created"
       EVENTS_BROKER=sns   → SnsEventPublisher   → SNS topic → SQS queue per consumer
   - UPDATE outbox_records SET status='PUBLISHED'

8. Broker delivers to all consumer groups simultaneously:

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

## Service discovery

This boilerplate uses **platform DNS with environment variables** — no service registry (Eureka, Consul) is needed.

### Why no Eureka or Consul

The architecture deliberately avoids service-to-service HTTP calls. The gateway is the only component that calls downstream services, and services coordinate asynchronously through the broker. This means there are exactly **two downstream URLs** to manage across the entire system:

```
COMMAND_SERVICE_URL   — gateway → command-service
QUERY_SERVICE_URL     — gateway → query-service
```

Eureka and Consul solve N×N discovery (every service finding every other service). This system has 2×1 — the problem doesn't exist here.

### How resolution works per platform

**Docker Compose (local dev)**

Docker creates an internal network and assigns each service a DNS name matching its key in `docker-compose.yml`. No configuration needed.

```
COMMAND_SERVICE_URL=http://command-service:8081
QUERY_SERVICE_URL=http://query-service:8082
```

The gateway resolves `command-service` via Docker's embedded DNS. Changing the number of replicas (`--scale command-service=3`) requires a load balancer in front — use Nginx or HAProxy, or switch to Kubernetes.

**Kubernetes**

Set these in the gateway `Deployment` env vars. kube-dns resolves the name to a ClusterIP, which load-balances across all healthy pods automatically.

```
COMMAND_SERVICE_URL=http://command-service.default.svc.cluster.local:8081
QUERY_SERVICE_URL=http://query-service.default.svc.cluster.local:8082
```

Scaling command-service to 5 pods: the gateway URL stays the same — kube-dns + the `Service` resource handle it.

```
api-gateway pod
    │
    │  http://command-service.default.svc.cluster.local:8081
    ▼
K8s Service (ClusterIP)  ──► pod 1
                          ──► pod 2
                          ──► pod 3
```

**AWS ECS — internal ALB (recommended)**

Each ECS service registers targets behind an internal Application Load Balancer. ECS handles health checks and drains unhealthy tasks automatically.

```
COMMAND_SERVICE_URL=http://internal-alb.your-vpc.amazonaws.com/command
QUERY_SERVICE_URL=http://internal-alb.your-vpc.amazonaws.com/query
```

**AWS ECS — Cloud Map (lightweight alternative)**

ECS registers each task in AWS Cloud Map. Cloud Map provides DNS that resolves to healthy task IPs — no ALB overhead for internal traffic.

```
COMMAND_SERVICE_URL=http://command-service.your-namespace.local:8081
QUERY_SERVICE_URL=http://query-service.your-namespace.local:8082
```

### Summary

| Platform | Mechanism | Config |
|---|---|---|
| Docker Compose | Docker embedded DNS | Service name = key in `docker-compose.yml` |
| Kubernetes | kube-dns + ClusterIP Service | `service.namespace.svc.cluster.local` |
| ECS + ALB | Internal ALB target groups | ALB DNS name |
| ECS + Cloud Map | AWS Cloud Map DNS | Cloud Map namespace DNS |

In every case: only `COMMAND_SERVICE_URL` and `QUERY_SERVICE_URL` need to change. No code changes, no service registry to operate.

---

## Shared library responsibilities

Each shared library is a Spring Boot auto-configuration module — services include it as a dependency and it wires itself automatically via `AutoConfiguration.imports`.

| Library | Auto-wires | Key classes |
|---|---|---|
| `shared-logging` | MDC filter, Logback JSON + Loki config | `MdcFilter`, `MdcTaskDecorator` |
| `shared-telemetry` | OTel span → MDC bridge, virtual thread metrics | `TracingMdcSpanHandler`, `VirtualThreadMetrics` |
| `shared-security` | JWT resource server, tenant context filter | `TenantContextFilter`, `TenantContextHolder`, `SecurityAutoConfiguration` |
| `shared-db` | RLS interceptor, outbox writer, inbox deduplicator | `RlsDataSourceInterceptor`, `OutboxWriter`, `InboxDeduplicator` |
| `shared-events` | Event publisher + consumer (Kafka or SNS/SQS), DLQ handling, no-op fallback | `KafkaEventPublisher`, `KafkaEventConsumer`, `KafkaConsumerConfig` (retry + DLQ routing), `SnsEventPublisher`, `SqsEventConsumer` (backoff), `SqsDlqConsumer`, `SqsClientConfig`, `NoOpEventPublisher` |
| `shared-payments` | Stripe payment gateway with circuit breaker + retry, webhook verifier | `StripePaymentGateway`, `StripeWebhookVerifier` |
| `shared-resilience` | Circuit breaker + retry auto-config, Micrometer metrics binding, default configs | `ResilienceAutoConfiguration` |

---

## Switching brokers — Kafka vs SNS/SQS

### Kafka (default — local dev and self-hosted)

No extra config needed. Kafka starts with `make infra`. All services default to `EVENTS_BROKER=kafka`.

```
outbox-relay → KafkaEventPublisher → Kafka topic
                                          │
                   ┌──────────────────────┤
                   ▼                      ▼
         KafkaEventConsumer          KafkaEventConsumer
         (query-service)             (consumer-service)
               │                          │
               │ on failure:              │ on failure:
               │ DefaultErrorHandler      │ DefaultErrorHandler
               │ 3× exponential backoff   │ 3× exponential backoff
               ▼                          ▼
         example.created.dlq        example.created.dlq
               │                          │
               ▼                          ▼
         DlqConsumer                DlqConsumer
         (logs + metric)            (logs + metric)
```

### SNS/SQS (AWS production)

**Infrastructure prerequisites** (Terraform in `terraform-eda-boilerplate`):

```hcl
# One SNS topic per event type
resource "aws_sns_topic" "example_created" { name = "example-created" }

# One SQS queue + DLQ per consumer service
resource "aws_sqs_queue" "example_created_dlq" {
  name                      = "example-created-dlq"
  message_retention_seconds = 1209600  # 14 days
}

resource "aws_sqs_queue" "example_created" {
  name                       = "example-created"
  visibility_timeout_seconds = 90  # must be >= max backoff (90s)
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.example_created_dlq.arn
    maxReceiveCount     = 3
  })
}

# Subscribe the SQS queue to the SNS topic
resource "aws_sns_topic_subscription" "example_created" {
  topic_arn = aws_sns_topic.example_created.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.example_created.arn
}
```

**Service config** — set per service (env var or `application.yml`):

```bash
EVENTS_BROKER=sns

# outbox-relay and command-service (publishers)
SNS_TOPIC_EXAMPLE_CREATED=arn:aws:sns:us-east-1:123456789:example-created
AWS_REGION=us-east-1

# consumer-service and query-service (consumers)
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/example-created
SQS_DLQ_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/example-created-dlq
AWS_REGION=us-east-1
```

Uncomment the `sqs`/`sns` blocks in each service's `application.yml`.

**Credentials** — no explicit config needed in production. The AWS SDK resolves from ECS task role or EC2 instance role automatically. For local dev with LocalStack:

```bash
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
SQS_ENDPOINT_OVERRIDE=http://localhost:4566
SNS_ENDPOINT_OVERRIDE=http://localhost:4566
```

**Message flow (SNS/SQS):**

```
outbox-relay → SnsEventPublisher → SNS topic (example-created)
                                         │
                          ┌──────────────┤ fan-out
                          ▼              ▼
               SQS queue             SQS queue
               (query-service)       (consumer-service)
                    │                     │
                    │ SqsEventConsumer     │ SqsEventConsumer
                    │ polls every 1s       │ polls every 1s
                    │                     │
                    │ on failure:          │ on failure:
                    │ changeMessageVisibility (10s/30s/90s backoff)
                    │ after maxReceiveCount=3:
                    ▼                     ▼
               SQS DLQ               SQS DLQ
               (example-created-dlq) (example-created-dlq)
                    │                     │
                    ▼                     ▼
               SqsDlqConsumer        SqsDlqConsumer
               polls every 5s        polls every 5s
               logs + metric         logs + metric
               deletes from DLQ      deletes from DLQ
```

**Local dev with LocalStack:**

```bash
make infra-sns        # start LocalStack
make sns-setup        # create topics, queues, DLQs, subscription
# then set EVENTS_BROKER=sns and endpoint overrides in .env
```

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
