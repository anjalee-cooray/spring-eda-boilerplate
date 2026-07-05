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

| Feature | Kafka (`EVENTS_BROKER=kafka`) | SNS/SQS (`EVENTS_BROKER=sns`) |
|---|---|---|
| Publisher | `KafkaEventPublisher` | `SnsEventPublisher` |
| Consumer | `KafkaEventConsumer` + `DefaultErrorHandler` | `SqsEventConsumer` + `changeMessageVisibility` backoff |
| DLQ routing | `DeadLetterPublishingRecoverer` → `{topic}.dlq` Kafka topic | SQS Redrive Policy → DLQ queue (infra-level); non-retryable exceptions write to DLQ directly |
| Retry strategy | In-process exponential backoff (3 attempts: 1s→2s→4s); non-retryable bypasses retries | SQS re-delivery via visibility timeout (10s→30s→90s); non-retryable deletes + routes to DLQ |
| Per-entity ordering | Partition key = `tenantId:aggregateId` | `MessageGroupId = tenantId:aggregateId` on FIFO topics |
| Broker-level dedup | `enable.idempotence=true` (sequence numbers, broker deduplicates retried produces) | `MessageDeduplicationId = eventId` on FIFO topics (5-min window) |
| Consumer lag metric | `kafka.consumer.lag{group,topic,partition}` via AdminClient | `sqs.consumer.lag{queue,type}` via `GetQueueAttributes`; CloudWatch alarm as secondary |
| Backpressure | `KafkaBackpressureController` pauses `KafkaListenerContainers` | `SqsBackpressureController` sets `isPaused`; `SqsEventConsumer.poll()` skips receive |
| Non-retryable exceptions | `addNotRetryableExceptions(IllegalArgumentException, ...)` — straight to DLQ | `isNonRetryable()` cause-chain check — writes to DLQ queue + deletes from source |
| Local dev | Kafka in Docker Compose | LocalStack (`make infra-sns`) |

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

### Event Replay

Event replay re-publishes already-processed events from `outbox_records` back to the broker. It is the primary tool for three operational scenarios:

| Scenario | What happened | Replay scope |
|---|---|---|
| Handler bug fixed | Consumer processed events incorrectly; fix deployed | Event type + date range |
| New consumer added | Service needs to bootstrap from historical events | Full tenant or event type |
| Read model corrupted | DB wiped or projection logic changed | Full tenant |
| Outage recovery | Events missed during a broker/consumer outage | Date range |

#### Why replay is safe

Events are re-published with their **original `event_id`**. Each consumer's `inbox_records` table deduplicates by `event_id`:
- Consumers that already processed the event → skip it silently (idempotent)
- New or reset consumers → process it normally

No special replay-awareness is needed in consumer code. The idempotent consumer pattern makes replay safe by design.

#### Replay is broker-agnostic

Replay publishes via `EventPublisher`. The same replay job works for both Kafka and SNS/SQS — switch `EVENTS_BROKER` in config and replay behaviour is unchanged.

#### API

Replay is triggered via the `outbox-relay` service's operator API (not exposed through the public gateway):

```
POST /replay/jobs         — trigger a replay job (returns 202 + job ID)
GET  /replay/jobs/{id}    — poll progress
GET  /replay/jobs         — list all jobs (filter by ?tenantId=...)
```

The HTTP response returns immediately with a job ID. Replay runs asynchronously in a dedicated thread pool (`replay-*` threads) so the outbox poller continues uninterrupted.

#### Replay scopes

All parameters except `tenantId` and `requestedBy` are optional. Filters combine with AND:

```json
// Full tenant replay — rebuild all read models from scratch
{ "tenantId": "tenant-1", "requestedBy": "ops" }

// Specific event type — after fixing a handler bug
{ "tenantId": "tenant-1", "eventType": "example.created", "requestedBy": "ops" }

// Date range — recover events from an outage window
{ "tenantId": "tenant-1",
  "fromTimestamp": "2025-01-01T00:00:00Z",
  "toTimestamp":   "2025-01-02T00:00:00Z",
  "requestedBy":   "ops" }

// Specific outbox record IDs — targeted fix
{ "tenantId": "tenant-1",
  "specificIds": ["uuid1", "uuid2"],
  "requestedBy": "ops" }
```

#### Job lifecycle

```
POST /replay/jobs
  → status: PENDING  (job created, returned in response)
  → status: RUNNING  (async execution starts, totalEvents populated)
  → status: COMPLETED  (all events re-published)
     or
  → status: FAILED  (error mid-replay, replayedCount shows progress before failure)
```

`replay_jobs` table stores every job permanently — full audit trail of who triggered what, when, and the outcome.

#### Progress monitoring

```bash
# Trigger full tenant replay
make replay TENANT=tenant-1

# Poll progress
make replay-status JOB=<uuid>

# or raw curl:
curl http://localhost:8084/replay/jobs/<uuid> | jq
# {
#   "id": "...",
#   "status": "RUNNING",
#   "totalEvents": 1500,
#   "replayedCount": 300,
#   ...
# }
```

Prometheus metric: `replay.events.published` (counter) — tracks total events re-published across all jobs.

#### Replay flow

```
Operator → POST /replay/jobs
               │
               ▼
         ReplayJob created (PENDING)
         202 Accepted returned immediately
               │
               ▼ (async, replay-* thread)
         ReplayJobService.execute()
               │
               ├─ specificIds set? → outboxRepository.findAllByIdIn(ids)
               └─ query-based?     → outboxRepository.findPublishedForReplay(
                                       tenantId, eventType, from, to)
               │
               ▼
         For each OutboxRecord (ordered by createdAt ASC):
           Build EventEnvelope with original event_id
           eventPublisher.publish(envelope)   ← Kafka or SNS, same code
           job.replayedCount++
           persist every 100 events
               │
               ▼
         Consumers receive events
           inbox_records.isDuplicate(event_id)?
             YES → skip (already processed)
             NO  → process + markProcessed(event_id)
               │
               ▼
         job.status = COMPLETED
```

#### Replay vs DLQ redrive

| | Event Replay | DLQ Redrive |
|---|---|---|
| Source | `outbox_records` (permanent) | DLQ topic / DLQ queue (retention-limited) |
| Scope | Any scope — tenant, type, date, IDs | All messages currently in DLQ |
| Trigger | `POST /replay/jobs` | Kafka: `--reset-offsets`; SQS: AWS Console "Start DLQ redrive" |
| When to use | Bug fix, new consumer, model rebuild | Transient failures after recovery |
| Deduplication | Inbox dedup (original event_id) | Inbox dedup (original event_id) |

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

## Consumer Health Monitoring

Without visibility into consumer processing, a lagging or stuck consumer can go undetected for hours. This section documents the two layers of monitoring added for consumers.

### Layer 1 — Per-handler offset tracking (database)

`ConsumerHealthTracker.recordSuccess()` is called at the end of each successful `handle()` call. It writes to the `consumer_offsets` table in the same transaction as the business logic, so the offset is only advanced when processing actually succeeded.

```
consumer_offsets (consumer_name, event_type, tenant_id) →
    last_event_id    UUID
    last_event_at    TIMESTAMPTZ
    events_processed BIGINT
    last_error       TEXT
    last_error_at    TIMESTAMPTZ
```

**Health endpoint** — `GET /actuator/consumer-health`:
```json
[
  {
    "consumer": "ExampleCreatedHandler",
    "eventType": "example.created",
    "tenantId": "tenant-1",
    "lastEventAt": "2025-01-15T10:30:00Z",
    "lagSeconds": 4,
    "eventsProcessed": 1247,
    "lastError": "none"
  }
]
```

**Micrometer gauge** — `consumer.lag.seconds{consumer, event_type, tenant}`:
Reports how many seconds ago the last event was processed. Registered lazily on first successful event per combination. Query in Prometheus:
```promql
consumer_lag_seconds{consumer="ExampleCreatedHandler"} > 300
```
Alert: consumer hasn't processed an event in 5 minutes.

**Usage in handlers:**
```java
@Transactional
public void handle(EventEnvelope envelope) {
    if (deduplicator.isDuplicate(envelope.eventId())) return;
    // ... business logic ...
    deduplicator.markProcessed(envelope.eventId(), ...);
    healthTracker.recordSuccess(getClass().getSimpleName(),
            envelope.eventId(), envelope.eventType(), envelope.tenantId());
}
```

### Layer 2 — Kafka consumer group lag (AdminClient)

`KafkaConsumerLagMetrics` polls the Kafka AdminClient every 15 seconds (configurable via `app.kafka.lag-refresh-ms`) and exposes lag per partition as a gauge:

```
kafka.consumer.lag{group, topic, partition}
```

Lag = `latestOffset - committedOffset` per partition. A lag of 0 means the consumer is caught up. A growing lag means the consumer is processing slower than messages arrive.

**Alert rule:**
```promql
kafka_consumer_lag{group="example-consumer-service"} > 1000
```

This component only activates when `app.events.broker=kafka`.

### SNS/SQS equivalent — SqsConsumerLagMetrics

`SqsConsumerLagMetrics` (`shared/shared-events`) provides the same capability for the SNS/SQS path. It polls `SqsClient.getQueueAttributes()` on a schedule and registers two Micrometer gauges:

```
sqs.consumer.lag{queue=example-created,   type=source}  — visible + in-flight messages
sqs.consumer.lag{queue=example-created-dlq, type=dlq}   — DLQ depth (non-zero = operator action needed)
```

`getMaxDepth()` returns the source queue depth — used by `SqsBackpressureController`.

Per-handler staleness (`consumer.lag.seconds`) works identically on both paths — `ConsumerHealthTracker` is broker-agnostic. The Prometheus alert `ConsumerStale` fires for both Kafka and SNS/SQS.

**CloudWatch as a secondary alarm** (recommended for production):
```
Metric:    AWS/SQS ApproximateNumberOfMessagesVisible
QueueName: example-created
Threshold: > 10000 for 5 consecutive minutes
Action:    SNS → PagerDuty / OpsGenie
```
This fires even when the consumer service is down — it complements `sqs.consumer.lag` but does not replace it.

For local dev with LocalStack:
```bash
aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes \
  --queue-url <url> \
  --attribute-names ApproximateNumberOfMessages
```

---

## Event Store — Permanent Audit Log

### The gap in the pure outbox approach

`outbox_records` is a relay queue, not an event log:
- Status changes from `PENDING` → `PUBLISHED` (mutable)
- Rows are candidates for cleanup once published
- Replay requires Kafka log retention to still hold the messages
- No queryable history of what happened and when

The `event_store` table fills this gap.

### What the event store provides

| Capability | How |
|---|---|
| Full audit trail | Every event ever emitted, tenant-scoped, timestamped, never deleted |
| Event sourcing | All events for one aggregate in order → reconstruct its state |
| Durable replay | Re-publish from event_store, independent of broker retention |
| Root cause analysis | Trace a saga or request via `correlation_id` across event types |

### Atomicity guarantee

`EventStoreWriter` is called inside `OutboxWriter.write()` under `@Transactional(MANDATORY)`, so both writes happen in the same transaction as the domain write:

```
@Transactional
handleCommand() {
    domainRepository.save(entity)     ─┐
    outboxWriter.write(eventType, ...) │  one transaction
      ├─ outbox_records INSERT         │  commit or rollback together
      └─ event_store INSERT           ─┘
}
```

An event either appears in **both** tables or **neither**. There is no state where the relay has a record that the event store does not.

### Schema

```sql
event_store (
    id             UUID PK,
    event_id       UUID UNIQUE,        -- stable business ID (same as outbox event_id)
    event_type     TEXT,
    tenant_id      TEXT,
    aggregate_id   TEXT,               -- optional: e.g. "order-uuid"
    aggregate_type TEXT,               -- optional: e.g. "Order"
    schema_version TEXT,
    payload        JSONB,
    correlation_id TEXT,
    causation_id   TEXT,
    occurred_at    TIMESTAMPTZ
)
```

**aggregate_id / aggregate_type** are optional fields for event-sourcing scenarios. When set, `EventStoreRepository.findByAggregateIdOrderByOccurredAtAsc()` returns the full event stream for that entity.

### Usage patterns

**Audit query — what happened to booking X?**
```sql
SELECT event_type, payload, occurred_at
FROM event_store
WHERE tenant_id = 'tenant-1'
  AND correlation_id = '<booking-correlation-id>'
ORDER BY occurred_at;
```

**Event sourcing — reconstruct Order aggregate:**
```java
List<EventStoreRecord> events =
    eventStoreRepository.findByAggregateIdOrderByOccurredAtAsc(orderId);
// apply events to an empty Order shell to rebuild current state
```

**Durable replay — re-publish without broker dependency:**
```java
// query event_store directly (not outbox) for replay
eventStoreRepository.findByTenant(tenantId, eventType, from, to, pageable)
    .forEach(record -> eventPublisher.publish(buildEnvelope(record)));
```

### Relation to outbox replay

`ReplayJobService` currently reads from `outbox_records WHERE status = 'PUBLISHED'`. This means replay only works while those rows exist (before cleanup). By querying `event_store` instead, replay becomes independent of outbox lifecycle — you can clean up PUBLISHED outbox records without losing the ability to replay.

---

## Saga Orchestration

### Choreography vs Orchestration

The boilerplate ships with **choreography** as the default pattern: each service reacts to events independently, with no central coordinator. This works well for simple flows.

For complex flows, **saga orchestration** is the right tool:

| Signal | Use choreography | Use orchestration |
|---|---|---|
| Steps | 1–2 | 3+ across multiple services |
| Compensation | Not needed | Required (rollback on failure) |
| Branching | None | Conditional paths |
| Visibility | Hard to trace | Saga status in DB |
| Coupling | Fully decoupled | Orchestrator knows participants |

### DB-backed saga orchestrator (no external service required)

The boilerplate provides a lightweight, DB-backed saga orchestrator. It does not require Temporal, Axon, or Conductor — it uses the existing Transactional Outbox and the `saga_instances` table.

**Key properties:**
- **Crash-safe** — all state transitions write to `saga_instances` in the same transaction as the next outbox record. Crash before commit → both are rolled back. Crash after commit → next response event reloads the saga from DB by `correlation_id`.
- **Idempotent** — duplicate response events are ignored (saga checks current step + terminal status).
- **Auditable** — the full saga lifecycle (step transitions, failure reason) is visible in `saga_instances`.

### Flow: ExampleBookingSaga

```
Trigger: booking.requested
  │
  ├─ saga created (STARTED)
  ├─ outbox: booking.inventory.reserve
  │
  ▼ AWAITING_INVENTORY
  ├─ booking.inventory.confirmed ──► AWAITING_PAYMENT
  │    └─ outbox: booking.payment.charge
  │         ├─ booking.payment.confirmed ──► COMPLETED
  │         └─ booking.payment.failed   ──► COMPENSATING
  │              └─ outbox: booking.inventory.release
  │                   └─ booking.inventory.released ──► COMPENSATED
  │
  └─ booking.inventory.failed ──► COMPENSATED (nothing to undo)
```

### Crash safety guarantee

```
handle(booking.inventory.confirmed) {
    @Transactional {
        saga.advanceTo(AWAITING_PAYMENT)           // ─┐ same transaction
        sagaRepository.save(saga)                  //  │
        outboxWriter.write(payment.charge, ...)    // ─┘ commit or rollback together
    }
}
```

If the service crashes between the transaction commit and the broker publish:
- The outbox record is `PENDING` — the outbox relay re-publishes it
- The `payment.charge` event arrives at the payment participant
- The saga is in `AWAITING_PAYMENT` state — ready to handle the response

### saga_instances table

| Column | Purpose |
|---|---|
| `correlation_id` | Links all events in one saga run |
| `status` | `STARTED` / `RUNNING` / `COMPLETED` / `COMPENSATING` / `COMPENSATED` / `FAILED` |
| `current_step` | Step name the saga is currently waiting on |
| `context` | JSONB — saga-specific fields available at every step |
| `compensation_step_index` | Tracks progress through compensation steps |
| `failure_reason` | Last error recorded when entering COMPENSATING or FAILED |

### How to add a new saga

1. Define event type constants (trigger + step responses + compensation responses)
2. Create a class implementing `EventConsumer` — see `ExampleBookingSaga` for the template
3. In `start()`: create `SagaInstance`, write first command to outbox, save — all in one `@Transactional`
4. In `handle()`: switch on `eventType`, load saga by `correlationId`, run the step handler
5. Each step handler: update saga state + write next command to outbox in one `@Transactional`
6. Compensation handlers mirror the same pattern in reverse

For flows with 10+ steps, external system calls (HTTP/gRPC), or distributed timeouts, consider **Temporal** (Java SDK available) — it provides durable execution, retry policies, and activity timeouts out of the box.

---

## Ordered Event Processing — Kafka Partition Key Strategy

Kafka preserves order only **within a single partition**. If two events for the same entity (e.g. `order-123`) land on different partitions, consumers may process them out of order. This section defines how the boilerplate enforces per-entity ordering.

### Partition key derivation

`KafkaEventPublisher` computes the message key before sending:

```
partitionKey = tenantId + ":" + aggregateId   when aggregateId is set
partitionKey = tenantId                        fallback for non-entity events
```

**Why `tenantId:aggregateId` and not just `aggregateId`?**
Two tenants may have entities with the same UUID (the ID space is per-tenant). Using `tenantId:aggregateId` prevents events from different tenants colliding on the same partition, which would violate row-level security isolation at the broker layer.

### How to set the partition key

Pass the entity's primary ID as `aggregateId` when writing to the outbox:

```java
// In your command handler / domain service:
outboxWriter.write(
    "order.shipped",       // event type
    "1",                   // schema version
    payload,               // event payload
    correlationId,         // correlation ID
    order.getId().toString() // ← aggregateId: routes all Order events to same partition
);
```

When `aggregateId` is `null` (or the older two-arg `write` overloads are used), the publisher falls back to tenant-level routing — all events for the tenant share a partition. This is fine for low-volume or non-entity events.

### Outbox → relay → broker chain

The `aggregate_id` column is persisted in `outbox_records` (V012 migration). The relay reads it and passes it through to `EventEnvelope.aggregateId()`. `KafkaEventPublisher` then uses it as the Kafka message key. The key is also preserved during **replay** (`ReplayJobService` reads `record.getAggregateId()`) so replayed events land on the same partition as the originals.

### SNS/SQS equivalent — FIFO topics

`SnsEventPublisher` automatically detects FIFO topics by checking whether the topic ARN ends with `.fifo`. For FIFO topics it sets:

```
MessageGroupId = tenantId + ":" + aggregateId   (when aggregateId is set)
MessageGroupId = tenantId                        (fallback for non-entity events)
```

All events with the same `MessageGroupId` are delivered in FIFO order within an SQS FIFO queue — the direct equivalent of Kafka partition ordering. Standard SNS topics do not support `MessageGroupId` and deliver without ordering guarantees.

**Infrastructure requirement:** Use SNS FIFO topics (ARN suffix `.fifo`) and SQS FIFO queues for event types where per-entity ordering matters.

### Trade-offs

| Decision | Rationale |
|---|---|
| `tenantId:aggregateId` composite key | Prevents cross-tenant partition/group collisions — on both Kafka and SNS FIFO |
| Null → tenant-level fallback | Backward compatible with pre-V012 events |
| Stored in `outbox_records.aggregate_id` | Relay and replay both use the same key — consistent routing on re-delivery |
| FIFO detected by ARN suffix | No extra config; ARN convention (`*.fifo`) is deterministic |
| Standard topics → no ordering | Standard SNS/SQS cannot provide ordering; FIFO topics are required |

---

## Publisher-Side Event Deduplication

The boilerplate provides a three-layer deduplication chain to prevent the same event from being processed by consumers more than once:

### Layer 1 — Outbox record uniqueness (DB constraint)

`outbox_records.event_id` has a `UNIQUE` constraint (V002). Attempting to insert two outbox records with the same `event_id` fails at the database level. Because `OutboxWriter.write()` generates a fresh `UUID.randomUUID()` per call, this prevents duplicate entries from concurrent writes in the same request.

### Layer 2a — Kafka idempotent producer (broker level, Kafka path)

With `enable.idempotence=true` and `acks=all`, Kafka assigns each producer a **producer ID** and a **sequence number** per partition. If the broker receives two `ProduceRequest` calls with the same sequence number (e.g. after a network timeout and retry), it deduplicates at the broker and acknowledges only once. This prevents the relay from publishing the same message twice during transient network failures.

Configuration applied to all Kafka producer configs:

```yaml
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5   # max allowed with idempotence=true
```

### Layer 2b — SNS FIFO MessageDeduplicationId (broker level, SNS/SQS path)

For SNS FIFO topics, `SnsEventPublisher` sets `MessageDeduplicationId = eventId.toString()` on every `PublishRequest`. SNS FIFO deduplicates messages with the same deduplication ID within a **5-minute window** — if the outbox relay publishes the same `event_id` twice (crash between Phase 2 and Phase 3), the second copy is silently discarded at the SNS broker.

This is the SNS/SQS equivalent of `enable.idempotence=true`. Standard SNS topics do not support `MessageDeduplicationId` and have no broker-level deduplication guarantee — the consumer `inbox_records` (Layer 3) remains the sole safety net on the standard path.

### Layer 3 — Consumer inbox deduplication (application level)

`InboxDeduplicator` writes every processed `event_id` to `inbox_records` in the same transaction as the domain write. If the same `event_id` arrives again (e.g. the relay published before crashing and then re-published after restart), the inbox check returns a duplicate and the handler is skipped silently.

The relay always preserves the **original** `event_id` from `outbox_records.event_id` — it never generates a new one. This is what makes Layer 3 effective: at-least-once duplicates carry the same `event_id` and are caught by the inbox.

### Deduplication chain summary

```
OutboxWriter.write()
  └─ DB UNIQUE(event_id) prevents duplicate outbox records
        │
        ▼
OutboxRelayPoller (Phase 2: publish)
  └─ Kafka idempotent producer (enable.idempotence=true)
        │
        ▼
KafkaEventConsumer / SqsEventConsumer
  └─ InboxDeduplicator checks inbox_records UNIQUE(event_id)
        └─ duplicate → skip handler (no business logic runs twice)
```

All three layers must be present. Removing any one layer creates a window where duplicates reach business logic.

---

## Backpressure — Consumer Lag Control

When a consumer processes events slower than they are published, lag accumulates on the Kafka partition (or SQS queue). Without a backpressure mechanism, the consumer silently falls further behind until broker retention expires and events are lost.

### Kafka: pause/resume via `KafkaBackpressureController`

`KafkaBackpressureController` runs on a configurable schedule and checks the maximum lag across all tracked partitions via `KafkaConsumerLagMetrics.getMaxLag()`.

```
lag > lagPauseThreshold  → pause all KafkaListenerContainers
lag < lagResumeThreshold → resume all KafkaListenerContainers
```

This creates a simple bang-bang controller: the consumer stops fetching from Kafka entirely when overwhelmed, giving the processing thread pool time to drain the in-flight batch. Once lag drops below the resume threshold, polling resumes automatically.

```yaml
app:
  consumer:
    backpressure:
      lag-pause-threshold:  10000   # pause when > 10k messages behind
      lag-resume-threshold: 1000    # resume when < 1k messages behind
      check-interval-ms:    5000    # evaluate every 5 seconds
```

The controller emits a `consumer.backpressure.active` Micrometer gauge (1 = paused, 0 = running) for alerting.

### Kafka: consumer tuning knobs

When lag is persistent (not a transient spike), tune the consumer before adjusting the pause threshold:

| Property | Default | Effect |
|---|---|---|
| `max.poll.records` | 500 | Increase to process more records per poll loop — up to memory limits |
| `fetch.max.bytes` | 52428800 (50 MB) | Cap fetch size to prevent OOM on large batches |
| `max.poll.interval.ms` | 300000 (5 min) | Must exceed the longest `handle()` call — if exceeded, broker revokes partition |

```yaml
spring:
  kafka:
    consumer:
      properties:
        max.poll.records: 500
        fetch.max.bytes: 52428800
        max.poll.interval.ms: 300000
```

### SQS: adaptive polling via `SqsBackpressureController`

`SqsBackpressureController` (`shared/shared-events`) reads `SqsConsumerLagMetrics.getMaxDepth()` — the source queue depth (visible + in-flight) polled from `SqsClient.getQueueAttributes()`. When depth exceeds `lag-pause-threshold`, it sets an internal `AtomicBoolean isPaused = true`. `SqsEventConsumer.poll()` checks `backpressureController.isPaused()` at the top of each scheduled invocation and returns early when paused — no `ReceiveMessage` call is made. Polling resumes automatically when depth drops below `lag-resume-threshold`.

Emits the same `consumer.backpressure.active` gauge as `KafkaBackpressureController` — the Prometheus `BackpressureActive` alert fires for both broker paths from a single rule.

**CloudWatch alarm (secondary, fires even when service is down):**
```
Metric:    AWS/SQS ApproximateNumberOfMessagesVisible
QueueName: example-created
Threshold: > 10000 for 5 consecutive minutes
Action:    SNS → PagerDuty / OpsGenie
```

### Non-retryable fast-path — SQS

`SqsEventConsumer` classifies exceptions before deciding how to handle them:

```
Retryable   → extend visibility timeout (exponential backoff: 10s → 30s → 90s)
              AWS Redrive Policy routes to DLQ after maxReceiveCount deliveries
Non-retryable → write to DLQ immediately + delete from source queue
              Skip all retry attempts (mirrors KafkaConsumerConfig.addNotRetryableExceptions())
```

Non-retryable exception types:
```java
IllegalArgumentException   // business validation failure — retrying will not fix it
IllegalStateException      // state machine invariant violated
JsonParseException         // malformed JSON payload — will never deserialise
```

`isNonRetryable()` walks the full cause chain, so non-retryable types wrapped in `RuntimeException` by handler code are also caught.

### Prometheus alert rules

Alert rules are defined in `docker/prometheus/alert_rules.yml` (loaded by `prometheus.yml` via `rule_files`). Rules apply to both Kafka and SNS/SQS paths unless noted:

| Alert | Expression | Severity | Applies to |
|---|---|---|---|
| `KafkaConsumerHighLag` | `kafka_consumer_lag > 10000` for 2 min | warning | Kafka only |
| `KafkaConsumerCriticalLag` | `kafka_consumer_lag > 100000` for 5 min | critical | Kafka only |
| `ConsumerStale` | `consumer_lag_seconds > 300` for 1 min | warning | **Both** |
| `BackpressureActive` | `consumer_backpressure_active == 1` for 5 min | warning | **Both** |
| `OutboxFailedRecords` | `outbox_records_failed > 0` for 0 s | critical | **Both** |
| `OutboxRelayNotPublishing` | `rate(outbox_relay_published_total[5m]) == 0 AND outbox_records_pending > 0` for 2 min | critical | **Both** |
| `DlqMessagesAccumulating` | `rate(events_dlq_received_total[10m]) > 0.1` for 5 min | warning | **Both** |

For SQS queue depth alerting beyond `consumer.backpressure.active`, add a CloudWatch alarm on `ApproximateNumberOfMessagesVisible` — it fires even when the consumer service is down.

---

## Role-Based Access Control (RBAC)

Every HTTP endpoint is protected by `@PreAuthorize` method-level security. `@EnableMethodSecurity` in `SecurityAutoConfiguration` activates Spring Security's AOP interceptors. Authorization is evaluated **after** authentication — a valid JWT is always required, then the role check runs.

### JWT roles claim → Spring Security authorities

The default `JwtAuthenticationConverter` only reads the `scope` / `scp` claim. Without explicit configuration, `hasRole('TENANT_ADMIN')` always returns false even when the JWT contains the right role. `SecurityAutoConfiguration` registers a custom converter:

```java
JwtGrantedAuthoritiesConverter rolesConverter = new JwtGrantedAuthoritiesConverter();
rolesConverter.setAuthoritiesClaimName("roles");   // reads the "roles" array from the JWT
rolesConverter.setAuthorityPrefix("ROLE_");        // maps "TENANT_ADMIN" → GrantedAuthority("ROLE_TENANT_ADMIN")
```

The `@PreAuthorize("hasRole('TENANT_ADMIN')")` SpEL expression then matches `ROLE_TENANT_ADMIN` in the authority set.

### Roles

| Role | JWT claim value | Services | Granted access |
|---|---|---|---|
| `TENANT_ADMIN` | `"TENANT_ADMIN"` | command-service, query-service | All tenant endpoints |
| `TENANT_MEMBER` | `"TENANT_MEMBER"` | command-service, query-service | All tenant endpoints |
| `PLATFORM_OPERATOR` | `"PLATFORM_OPERATOR"` | outbox-relay | Replay job management |

### Endpoint authorization matrix

| Endpoint | Service | Required role |
|---|---|---|
| `POST /commands/examples` | example-command-service | `TENANT_ADMIN` or `TENANT_MEMBER` |
| `GET /queries/examples` | example-query-service | `TENANT_ADMIN` or `TENANT_MEMBER` |
| `GET /queries/examples/{id}` | example-query-service | `TENANT_ADMIN` or `TENANT_MEMBER` |
| `POST /replay/jobs` | outbox-relay | `PLATFORM_OPERATOR` |
| `GET /replay/jobs/{id}` | outbox-relay | `PLATFORM_OPERATOR` |
| `GET /replay/jobs` | outbox-relay | `PLATFORM_OPERATOR` |
| `/actuator/health`, `/actuator/info`, `/actuator/prometheus` | all | public (no token) |

### Replay endpoint security

The replay API was previously annotated as "internal — restrict via VPC or mTLS." This is now enforced via JWT: requests to `/replay/jobs/**` require a token with `roles: ["PLATFORM_OPERATOR"]`. The OIDC issuer URI is configured via `OAUTH2_ISSUER_URI` in `outbox-relay`'s `application.yml`. VPC-level restriction remains a defence-in-depth layer on top.

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
# 1. Create an entity — Idempotency-Key is required for all mutating commands
IDEM_KEY=$(uuidgen)
RESPONSE=$(curl -s -X POST http://localhost:9080/api/commands/examples \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM_KEY" \
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

-- Replay jobs — audit trail of all replays triggered
SELECT id, tenant_id, event_type, status, total_events, replayed_count, requested_by, created_at
FROM replay_jobs ORDER BY created_at DESC LIMIT 10;

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

### Verify idempotency

```bash
IDEM_KEY=$(uuidgen)

# First request — creates the entity
curl -s -X POST http://localhost:9080/api/commands/examples \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -d '{"name":"idem-test"}' | jq
# {"id": "some-uuid"}

# Second request with the same key — returns cached response, no duplicate created
curl -sv -X POST http://localhost:9080/api/commands/examples \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -d '{"name":"idem-test"}' 2>&1 | grep "Idempotency-Status"
# Idempotency-Status: HIT

# Missing Idempotency-Key → 400
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:9080/api/commands/examples \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"no-key"}'
# 400
```

### Verify rate limiting

```bash
# Fire 50 rapid requests — after burst capacity (40) is exhausted, should get 429
for i in $(seq 1 50); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    http://localhost:9080/api/queries/examples \
    -H "Authorization: Bearer $TOKEN")
  echo "$i: $STATUS"
done
# First ~40: 200, then: 429 Too Many Requests
# Bucket refills at 20 req/s — wait 2s and try again to confirm recovery
```

### Verify DLQ handling

```bash
# To test DLQ, temporarily break a handler (e.g. throw in ExampleCreatedHandler),
# then publish an event. After 3 retries (1s + 2s + 4s), the message routes to the DLQ.

# Watch consumer-service logs for DLQ routing:
docker compose logs -f example-consumer-service | grep "DLQ"
# DLQ event received — operator action required dlqTopic=example.created.dlq ...

# Check the Prometheus counter (after Grafana scrapes):
curl -s http://localhost:8083/actuator/prometheus | grep events_dlq_received
# events_dlq_received_total{topic="example.created.dlq"} 1.0
```

### Verify event replay

```bash
# Trigger a full replay for tenant-1 (re-publishes all PUBLISHED outbox records)
make replay TENANT=tenant-1
# Returns: {"id":"<job-uuid>","status":"PENDING",...}

# Poll until COMPLETED
make replay-status JOB=<job-uuid>
# {"status":"RUNNING","totalEvents":5,"replayedCount":3,...}
# {"status":"COMPLETED","totalEvents":5,"replayedCount":5,...}

# Confirm inbox deduplication worked — consumer-service skipped already-processed events
docker compose logs example-consumer-service | grep "Skipping duplicate"
# Skipping duplicate event eventId=... eventType=example.created

# List all jobs
make replay-list TENANT=tenant-1
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

### Outbox Relay — At-Least-Once Guarantees Under Failure

The Transactional Outbox pattern guarantees an event is **written** atomically with the domain change. The relay's job is to guarantee it is **delivered** — even when the relay crashes, restarts, or runs as multiple concurrent instances.

#### Problems with the naïve approach

A simple `@Transactional poll()` that reads, publishes, and marks PUBLISHED in a single transaction has four failure modes:

| Failure | What goes wrong |
|---|---|
| Two relay pods run simultaneously | Both read the same PENDING records, both publish — double delivery |
| Crash after broker send, before DB commit | Record stays PENDING, re-published on restart — eventId is different each time so inbox deduplication is bypassed |
| Broker permanently unavailable | Record retried on every poll forever — no failure state, no alert |
| DB connection held during broker call | Long-running network I/O inside a transaction holds a connection pool slot |

#### Three-phase commit pattern

The relay uses a three-phase approach that eliminates all four problems:

```
Phase 1 — CLAIM (REQUIRES_NEW tx, commits before Phase 2)
  OutboxClaimService:
    SELECT id FROM outbox_records
    WHERE status = 'PENDING'
    ORDER BY created_at ASC
    LIMIT :batchSize
    FOR UPDATE SKIP LOCKED         ← concurrent relay pods see disjoint batches
    ;
    UPDATE outbox_records
    SET status = 'IN_FLIGHT',
        locked_until = now() + 30s,  ← dead-man's switch for crash recovery
        attempt_count = attempt_count + 1
    WHERE id IN (:claimedIds);
  ── transaction commits ──

Phase 2 — PUBLISH (no transaction, no DB connection held)
  eventPublisher.publish(buildEnvelope(record))
    envelope.eventId = record.eventId   ← ORIGINAL id, not auto-generated
    envelope.schemaVersion = record.schemaVersion

Phase 3 — COMPLETE (REQUIRES_NEW tx per record)
  On success  → status = PUBLISHED, locked_until = NULL
  On failure, attempts < max  → status = PENDING (retry on next poll)
  On failure, attempts ≥ max  → status = FAILED  (alert required)
```

#### SKIP LOCKED — concurrent relay safety

`FOR UPDATE SKIP LOCKED` is a PostgreSQL row-level locking hint. When relay pod A locks rows 1–50, relay pod B's identical query skips those rows and returns rows 51–100 instead of blocking. The result:

- Two relay instances double throughput without producing duplicates
- Rolling deploys are safe — old and new versions relay disjoint batches simultaneously
- No application-level distributed locking required

#### Crash recovery — the reclaimer

If the relay crashes between Phase 1 (claim) and Phase 3 (complete), the affected records stay `IN_FLIGHT` with a `locked_until` timestamp in the past. `OutboxReclaimTask` runs every 60 seconds (configurable via `app.relay.reclaim-interval-ms`):

```
find IN_FLIGHT records where locked_until < now()
for each:
  if attempt_count < max_attempts → PENDING  (re-enters Phase 1 on next poll)
  if attempt_count ≥ max_attempts → FAILED   (operator action required)
```

The reclaimer does not publish events directly — it only resets DB state. The normal poll loop handles the re-publish on the next cycle.

#### eventId preservation — why it matters

The relay always builds the envelope with `record.eventId` (the UUID written by the domain service at insert time). If the relay auto-generated a fresh UUID on every publish attempt:

1. Relay publishes event A with `event_id = X` → consumer processes, writes `event_id = X` to inbox_records
2. Relay crashes, restarts, re-publishes event A with `event_id = Y`
3. Consumer checks inbox_records for `Y` — not found — **processes the event a second time**

By using the original ID, step 3 finds `X` is a known duplicate only if the record was already processed under that exact ID — which it was.

#### Status lifecycle

```
        claim (SKIP LOCKED)
PENDING ──────────────────► IN_FLIGHT ──── publish success ───► PUBLISHED
  ▲                             │
  │          publish failure    │ (attempt_count < max_attempts)
  └─────────────────────────────┘
                                │ (attempt_count ≥ max_attempts)
                                └──────────────────────────────► FAILED
```

The reclaimer follows the same logic for expired IN_FLIGHT records.

#### Database columns

| Column | Type | Purpose |
|---|---|---|
| `status` | TEXT | `PENDING` / `IN_FLIGHT` / `PUBLISHED` / `FAILED` |
| `locked_until` | TIMESTAMPTZ | Dead-man's switch; NULL when not IN_FLIGHT |
| `attempt_count` | INTEGER | Incremented at claim time, not failure time |
| `last_error` | TEXT | Last exception message for operator debugging |

#### Metrics and alerting

| Metric | Type | Alert condition |
|---|---|---|
| `outbox.relay.published` | Counter | — |
| `outbox.relay.failed` | Counter | Any increment → P1 (event permanently lost) |
| `outbox.records.failed` | Gauge | > 0 → P1 (FAILED records need operator action) |

**Operator action for FAILED records:**
```sql
-- Find FAILED records
SELECT id, event_type, tenant_id, attempt_count, last_error, created_at
FROM outbox_records
WHERE status = 'FAILED'
ORDER BY created_at;

-- After fixing the root cause, reset for retry:
UPDATE outbox_records
SET status = 'PENDING', attempt_count = 0, last_error = NULL, locked_until = NULL
WHERE id = '<id>';
```

#### Configuration reference

| Property | Default | Description |
|---|---|---|
| `app.relay.batch-size` | 50 | Records claimed per poll |
| `app.relay.poll-interval-ms` | 1000 | Delay between polls |
| `app.relay.lock-timeout-seconds` | 30 | IN_FLIGHT lock duration (dead-man's switch) |
| `app.relay.max-attempts` | 5 | Attempts before FAILED |
| `app.relay.reclaim-interval-ms` | 60000 | Reclaimer scan frequency |

---

### Event Schema Versioning

Schema versioning lets you evolve event payloads over time without breaking consumers that were built against an older shape. The approach is **application-level versioning** — no external schema registry service is required.

#### How it works

Every `EventEnvelope` carries a `schema_version` field:

```json
{
  "event_id": "...",
  "event_type": "example.created",
  "tenant_id": "tenant-1",
  "schema_version": "1",
  "payload": { "id": "...", "name": "...", "status": "ACTIVE" }
}
```

**Publishers validate before sending.** `EventSchemaRegistry` loads JSON Schema files from `classpath:schemas/` at startup and validates the payload before publishing. If a schema file exists for the event type + version, validation is strict. If no schema file exists, validation is skipped (opt-in adoption).

**Consumers upcast before dispatching.** `EventUpcasterRegistry` applies a chain of `EventUpcaster` implementations to bring an incoming event up to the latest schema version before handler dispatch. The chain runs in version order: v1→v2 runs before v2→v3.

```
Publisher                        Broker                  Consumer
─────────                        ──────                  ────────
OutboxWriter                     Kafka / SQS             KafkaEventConsumer
  write(type, version, payload)                            deserialize envelope
  ↓                                                        ↓
EventPublisher.publish()                                 UpcasterRegistry.upcastToLatest()
  ↓                                                        ↓ (v1→v2, v2→v3 chain)
EventSchemaRegistry.validate()   ──── envelope ────>    EventConsumer.handle(current)
  (fail fast if invalid)
```

#### Schema file naming

Schema files live in `shared-events/src/main/resources/schemas/` and follow the convention:

```
schemas/{event_type}-v{N}.json
```

Examples:
```
schemas/example.created-v1.json
schemas/example.created-v2.json   ← add when bumping to v2
schemas/example.updated-v1.json
```

The `EventSchemaRegistry` parses the filename to extract the event type and version number.

#### Evolving a schema (step-by-step)

**Backward-compatible change** (adding an optional field — no version bump needed):
1. Add the new field with `additionalProperties: true` in the existing schema file.
2. No producer or consumer code changes required.

**Breaking change** (removing a field, renaming, or changing a type):
1. Increment the version in `EventVersion.java`:
   ```java
   private static final Map<String, String> CURRENT = Map.of(
       "example.created", V2,  // was V1
       ...
   );
   ```
2. Add a new schema file: `schemas/example.created-v2.json`
3. Keep `schemas/example.created-v1.json` — do not delete it.
4. Implement `EventUpcaster` in each consumer service:
   ```java
   @Component
   public class ExampleCreatedV1ToV2Upcaster implements EventUpcaster {
       public String eventType()   { return "example.created"; }
       public String fromVersion() { return "1"; }
       public String toVersion()   { return "2"; }

       public EventEnvelope upcast(EventEnvelope envelope) {
           Map<String, Object> payload = new LinkedHashMap<>((Map) envelope.payload());
           payload.put("displayName", payload.getOrDefault("name", ""));
           return EventEnvelope.builder()
               // ... copy identity fields ...
               .payload(payload)
               .schemaVersion("2")
               .build();
       }
   }
   ```
5. Deploy consumers before rolling out the new producer version (expand-then-contract).

#### Compatibility rules

| Change type | Version bump? | Upcaster needed? |
|---|---|---|
| Add optional field | No | No |
| Add required field with default | Yes | Yes (synthesise the default) |
| Remove field | Yes | Yes (consumers ignore it via `additionalProperties: true`) |
| Rename field | Yes | Yes (copy old → new, drop old) |
| Change field type | Yes | Yes (convert the value) |

#### Key classes

| Class | Location | Role |
|---|---|---|
| `EventSchemaRegistry` | `shared-events` | Loads schemas, validates on publish |
| `EventVersion` | `shared-events` | Maps event type → current version number |
| `EventUpcaster` | `shared-events` | Interface for a single version migration |
| `EventUpcasterRegistry` | `shared-events` | Chains upcasters; called before handler dispatch |
| `SchemaValidationException` | `shared-events` | Thrown when payload fails schema validation |
| `ExampleCreatedV1ToV2Upcaster` | `example-consumer-service` | Template upcaster (commented out) |

#### Replay and schema versions

Event replay re-publishes outbox records with their **original `schema_version`**. This means consumers always receive events at the version they were originally published with, regardless of the current schema version. The upcaster chain then normalises them before handler dispatch — making replay safe even across schema migrations.

---

## What each verification proves

| Test | What it proves |
|---|---|
| POST with Idempotency-Key → 201 | Command handler, RLS write, outbox write, idempotency filter accepted key |
| POST without Idempotency-Key → 400 | IdempotencyFilter enforcing key requirement on command routes |
| Retry with same key → HIT header | Redis idempotency cache hit, no duplicate entity created |
| GET returns entity | Outbox relay, broker, projector, read model projection |
| Status = ACTIVE | Choreography handler received, processed, and re-published the event |
| Outbox status = PUBLISHED | Relay polled and published successfully |
| Inbox records exist | Idempotent consumer deduplication is running |
| Rapid GETs → 429 after burst | Redis token bucket rate limiter enforcing per-tenant limits |
| DB query without tenant → 0 rows | RLS isolation enforced at database level |
| 401 without token | Gateway JWT validation |
| DLQ log line appears | DefaultErrorHandler retried and routed to DLQ after exhaustion |
| events_dlq_received counter > 0 | Micrometer counter wired to DLQ consumer |
| Replay job → COMPLETED | ReplayJobService queried outbox, re-published via EventPublisher |
| Consumer logs "Skipping duplicate" | Inbox deduplication made replay safe — already-processed events skipped |
| Actuator health returns UP | All services healthy |
| Grafana trace links to logs | OTel + Loki + Tempo wired correctly |
| Invalid payload → SchemaValidationException | EventSchemaRegistry enforcing JSON Schema before publish |
| Old event (v1) handled by v2 consumer | EventUpcasterRegistry chain normalised schema before dispatch |
| GET /actuator/consumer-health → lagSeconds | ConsumerHealthTracker updated after every successful handle() |
| consumer.lag.seconds gauge rising | Consumer is stuck or processing slower than publish rate |
| kafka.consumer.lag{partition} > 0 | Kafka AdminClient lag metric wired and refreshing |
| event_store has same rows as outbox | OutboxWriter writes both tables atomically |
| findByAggregateId returns ordered events | Event sourcing query working via aggregate_id index |
| Saga status = COMPENSATED after payment.failed | ExampleBookingSaga ran compensation step successfully |
| Two relay instances, zero duplicate deliveries | SKIP LOCKED claim ensures disjoint batches per pod |
| Kill relay mid-poll, record still delivered | ReclaimTask reset expired IN_FLIGHT to PENDING on restart |
| outbox.records.failed gauge > 0 | Record exhausted all attempts — alert fires, operator resets |
| SNS FIFO publish with aggregateId → same MessageGroupId | Per-entity ordering via SnsEventPublisher.partitionKey() |
| Publish same event_id twice to FIFO topic → only one delivery | MessageDeduplicationId dedup working at SNS broker |
| sqs.consumer.lag{type=source} gauge > 0 | SqsConsumerLagMetrics.refreshDepth() polling SQS |
| SQS queue depth > lagPauseThreshold → poll skipped | SqsBackpressureController paused, SqsEventConsumer returns early |
| SQS poll resumes after depth < lagResumeThreshold | Backpressure auto-resume working |
| IllegalArgumentException from handler → DLQ, no visibility extension | Non-retryable fast-path in SqsEventConsumer |
| consumer.backpressure.active gauge == 1 during SQS pause | Same gauge works for both Kafka and SNS/SQS paths |
