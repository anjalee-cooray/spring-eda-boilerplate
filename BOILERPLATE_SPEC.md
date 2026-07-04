# spring-eda-boilerplate — Architecture Specification

## Overview

A production-ready Spring Boot microservices boilerplate for building multi-tenant, event-driven SaaS applications on Java 25. All core architectural decisions are pre-made and wired together. Everything external is pluggable behind interfaces.

---

## Architecture

### Style

Event-Driven Microservices with CQRS. Synchronous flows use HTTP (JWT-authenticated via the API gateway). Asynchronous flows use a message broker (Kafka or SNS/SQS) via the Transactional Outbox pattern.

### Services

| Service | Port | Responsibility |
|---|---|---|
| `api-gateway` | 9080 | JWT validation, tenant context propagation, rate limiting, request routing |
| `example-command-service` | 8081 | Command handling, domain writes, event choreography, outbox writes |
| `example-query-service` | 8082 | CQRS read model, event projection, query endpoints |
| `example-consumer-service` | 8083 | Idempotent event consumer — pattern for notification/billing/audit services |
| `outbox-relay` | 8084 | Polls `outbox_records`, publishes events to the broker |
| `db-migrations` | — | Short-lived Flyway job — runs before any service deploys |

### Shared Libraries

| Module | Purpose |
|---|---|
| `shared-logging` | Logback JSON config, MDC filter (`tenant_id`, `correlation_id`, `trace_id`), Loki appender |
| `shared-telemetry` | OTel config, Micrometer Prometheus registry, virtual thread metrics, trace→MDC span handler |
| `shared-security` | OIDC JWT filter, `TenantContext`, `TenantContextHolder`, role constants |
| `shared-db` | RLS `SET LOCAL` interceptor, `OutboxWriter`, `InboxDeduplicator`, JPA base entities |
| `shared-events` | `EventEnvelope` record, `EventPublisher` interface, Kafka + SNS/SQS implementations |
| `shared-payments` | `PaymentGateway` interface, Stripe implementation, webhook verifier |

---

## Core Patterns

### Multi-Tenancy — PostgreSQL RLS

Every tenant-scoped table has an RLS policy:

```sql
CREATE POLICY tenant_isolation ON table_name
    USING (tenant_id = current_setting('app.tenant_id', true));
```

`RlsDataSourceInterceptor` calls `SET LOCAL app.tenant_id = ?` before every Spring Data repository call. Safe default: if no tenant context is set, the query returns zero rows — never all rows.

### Transactional Outbox

Command handlers write domain records and outbox records in a single transaction:

```
BEGIN
  INSERT INTO example_entities ...
  INSERT INTO outbox_records (event_type, payload, status='PENDING') ...
COMMIT
```

`outbox-relay` polls `outbox_records` for `PENDING` records, publishes each via `EventPublisher`, then marks it `PUBLISHED`. Broker failure never loses an event — the outbox record remains `PENDING` until successfully published.

### Idempotent Consumer

Before processing an event, consumers call `InboxDeduplicator.isDuplicate(eventId)`. If true, the event is skipped. After processing, `markProcessed(eventId)` inserts into `inbox_records` within the same transaction. Duplicate delivery from the broker is harmless.

### CQRS

CQRS covers only two services — command and query. The consumer service is **not** part of CQRS; it is a separate event-driven pub/sub pattern.

| Service | Pattern | Purpose |
|---|---|---|
| `example-command-service` | CQRS — command side | Domain writes, business rules, event publishing |
| `example-query-service` | CQRS — query side | Read model projections, query endpoints |
| `example-consumer-service` | Event-Driven Pub/Sub | Side effects — the pattern for notification, billing, audit services |

Command and query services never share a database connection. The command service writes to `example_entities`. The query service maintains `example_read_models` by consuming domain events from the broker and projecting them via `ExampleProjector`. Query endpoints read only from the read model.

### Event Choreography

`ExampleChoreographyHandler` in the command service demonstrates event choreography — it implements `EventConsumer`, subscribes to `example.created`, activates the entity, and publishes `example.activated`. No central coordinator; each service reacts to events and produces the next one.

```
command-service publishes "example.created"
  └─ ExampleChoreographyHandler receives it → activates entity → publishes "example.activated"
     └─ query-service projects it → updates read model
     └─ notification-service sends confirmation email
```

**Choreography vs Orchestration:**

| | Choreography (this boilerplate) | Orchestration |
|---|---|---|
| Coordinator | None — services react independently | Central saga class tracks each step |
| State tracking | No saga state needed | Requires `saga_instances` DB table |
| Coupling | Services only know about events | Orchestrator knows all participants |
| Best for | Linear flows, loose coupling | Complex flows with branching, retries, compensation across many services |
| Frameworks | None needed | Axon, Conductor, Temporal |

Use choreography by default. Upgrade to orchestration when you need to track saga state across many services, handle partial failures with compensation, or manage branching flows that choreography cannot express cleanly.

### Virtual Threads

Project Loom is enabled globally via `--enable-preview` in all Dockerfiles and Gradle config. Every service benefits from virtual threads for I/O-bound workloads without any code changes.

---

## Pluggable Points

### Messaging

Set `app.events.broker` in `application.yml`:

```yaml
app:
  events:
    broker: kafka   # or: sns
```

- `kafka` — activates `KafkaEventPublisher` and `KafkaEventConsumer`
- `sns` — activates `SnsEventPublisher` and `SqsEventConsumer`

Implement `EventPublisher` and `EventConsumer` to add a new broker.

### Auth (OIDC)

Set `OIDC_ISSUER_URI` per environment:

```
# Local dev (mock-oauth2-server)
OIDC_ISSUER_URI=http://localhost:8090/okta

# Okta staging
OIDC_ISSUER_URI=https://dev-xxx.okta.com/oauth2/default

# Auth0
OIDC_ISSUER_URI=https://your-tenant.auth0.com/
```

No code changes required. Spring Security auto-configures the JWKS endpoint from the issuer URI.

**Okta claim mapping** (configure via Okta Expression Language in the authorization server):
- `tenant_id` — custom claim identifying the tenant
- `roles` — custom claim mapped from Okta groups

### Payments

Set `app.payments.provider` in `application.yml`:

```yaml
app:
  payments:
    provider: stripe  # default
```

Implement `PaymentGateway` to add another provider. Wire the new implementation with `@ConditionalOnProperty(name = "app.payments.provider", havingValue = "your-provider")`.

### Cloud

- **AWS** — ECS Fargate, RDS PostgreSQL, ElastiCache Redis, MSK Kafka or SNS+SQS, Secrets Manager
- **Azure** — Azure Container Apps, Azure Database for PostgreSQL, Azure Cache for Redis, Azure Event Hubs, Key Vault

Terraform modules for both clouds are in [`terraform-eda-boilerplate`](https://github.com/anjalee-cooray/terraform-eda-boilerplate).

---

## Database Schema

### Migrations (Flyway)

| Version | File | Contents |
|---|---|---|
| V001 | `create_tenants.sql` | `tenants` table |
| V002 | `create_outbox_records.sql` | `outbox_records` table + indexes |
| V003 | `create_inbox_records.sql` | `inbox_records` table + indexes |
| V004 | `enable_rls.sql` | RLS policies, `app_user` and `platform_operator` roles |
| V005 | `create_example_tables.sql` | `example_entities` (write model) + `example_read_models` (read model) |

### Roles

| Role | RLS | Purpose |
|---|---|---|
| `app_user` | Subject to RLS | All application services |
| `platform_operator` | Bypasses RLS | Operator console, migration runner |

---

## Event Envelope

Every domain event travels in a standard envelope:

```json
{
  "event_id": "uuid",
  "event_type": "example.created",
  "tenant_id": "tenant-1",
  "correlation_id": "uuid",
  "causation_id": "uuid-or-null",
  "occurred_at": "2025-01-01T00:00:00Z",
  "payload": { }
}
```

`correlation_id` links all events in a saga. `causation_id` links a response event to its triggering event.

---

## Observability

| Signal | Tool | How |
|---|---|---|
| Traces | Tempo (OTel) | OTel Java agent attached to every service via `-javaagent`. 100% sampling. |
| Logs | Loki | `loki-logback-appender` pushes structured JSON logs. Labels: `service`, `level`, `tenant_id`. |
| Metrics | Prometheus + Grafana | Micrometer `/actuator/prometheus` scraped every 15s. |
| Dashboards | Grafana | Service health dashboard auto-provisioned at startup. |

Trace ID is written into MDC at span start via `TracingMdcSpanHandler` — every log line carries `trace_id`, enabling trace-to-log correlation in Grafana.

**Local URLs:**
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Tempo: http://localhost:3200
- Loki: http://localhost:3100

---

## Security

### JWT Validation

All requests pass through the API gateway which validates JWTs against the OIDC issuer. Invalid tokens return 401 before reaching any service.

### Tenant Context Propagation

The gateway extracts `tenant_id` from the JWT and forwards it as `X-Tenant-Id`. Downstream services read it via `TenantContextFilter` into `TenantContextHolder` (ThreadLocal). `RlsDataSourceInterceptor` then sets `app.tenant_id` on the DB connection before every query.

### Actuator Endpoints

`/actuator/health`, `/actuator/info`, and `/actuator/prometheus` are public on all services. All other endpoints require a valid JWT.

---

## CI/CD

### PR Pipeline (`pr-pipeline.yml`)

```
Checkstyle → Unit Tests → Integration Tests (Testcontainers) → Build & Push Images (GHCR)
```

### Deploy Pipeline (`deploy-backend.yml`)

```
Build & Push Images → Run Migrations (db-migrations job) → Rolling Deploy → Smoke Test → Rollback on failure
```

Deploy target stubs are in each pipeline step — replace with your ECS / Kubernetes commands.

### Required Secrets

See [`.github/SECRETS.md`](.github/SECRETS.md).

---

## Local Dev

### Prerequisites

- Docker + Docker Compose
- Java 25 (Temurin)
- Gradle (wrapper included)

### Quickstart

```bash
# 1. Start infrastructure
make infra

# 2. Build all JARs
./gradlew build -x test

# 3. Build Docker images
docker compose build

# 4. Start the full stack
make up

# 5. Tail logs
make logs
```

### Test Users (mock-oauth2-server)

The mock OIDC server issues tokens with `tenant_id: tenant-1` and `roles: [TENANT_ADMIN]` for any login. Customise claims in the `JSON_CONFIG` env var in `docker-compose.yml`.

Get a token:

```bash
curl -s -X POST http://localhost:8090/okta/token \
  -d "grant_type=client_credentials&client_id=test&client_secret=test&scope=openid" \
  | jq -r '.access_token'
```

### Service Ports

| Service | Port |
|---|---|
| API Gateway | 9080 |
| Command Service | 8081 |
| Query Service | 8082 |
| Consumer Service | 8083 |
| Outbox Relay | 8084 |
| Mock OIDC | 8090 |
| PostgreSQL | 5432 |
| Redis | 6379 |
| Kafka | 9092 |
| Grafana | 3000 |
| Prometheus | 9090 |
| Loki | 3100 |
| Tempo | 3200 |

---

## Incremental Adoption

You do not need to run all services from day one. The services are independent — start with only what you need and introduce the rest as the product grows.

### Stage 1 — Command service only

Run only the command service. Add GET endpoints directly to it so you can read data without a query service. The outbox relay and broker are not needed yet — `NoOpEventPublisher` activates automatically when no broker is configured, logging events instead of publishing them. Outbox records accumulate as `PENDING` and are harmless.

```bash
# Minimal infrastructure
docker compose up postgres redis mock-oidc grafana prometheus

# Run the command service
./gradlew :services:example-command-service:bootRun
```

Add reads to the command controller:

```java
@GetMapping("/commands/examples/{id}")
public ResponseEntity<ExampleEntity> get(@PathVariable UUID id) {
    return repository.findById(id)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
}
```

### Stage 2 — Add async messaging

When you need background reactions to events (emails, webhooks, side effects), add Kafka and the outbox relay:

```bash
docker compose up kafka outbox-relay
```

Set `app.events.broker: kafka` in the command service. The relay will drain the accumulated `PENDING` outbox records on startup.

### Stage 3 — Add CQRS read model

When your read requirements diverge from the write model — different shape, heavy aggregation, or independent scaling — introduce the query service and write projections.

### Stage 4 — Add dedicated consumer service

When you have enough event-driven side effects to justify a separate deployment, move handlers out of the query service into a dedicated consumer service.

---

## Adopting This Boilerplate

1. **Rename packages** — replace `com.example.eda` with your group ID
2. **Replace example domain** — delete `example-*` classes, add your domain entities and commands
3. **Configure Okta** — set `OIDC_ISSUER_URI` to your Okta authorization server, add `tenant_id` and `roles` claims via Expression Language
4. **Configure broker** — set `app.events.broker` to `kafka` or `sns`, add connection config
5. **Add migrations** — add `V006__create_your_domain.sql` following the existing RLS pattern
6. **Wire deploy target** — replace the stub `echo` commands in the deploy pipeline with your ECS / Kubernetes deploy commands
7. **Publish shared libs** (optional, for multi-repo evolution) — publish `shared-*` modules to a private Maven registry with semantic versioning when teams split

---

## What This Boilerplate Does Not Include

- API versioning strategy
- Rate limiting implementation (stub exists in gateway `application.yml`)
- Tenant provisioning flow (tenants table exists, provisioning logic is your domain)
- Email / notification service
- File storage service
- Frontend (see [`next-eda-boilerplate`](https://github.com/anjalee-cooray/next-eda-boilerplate))
- Terraform infrastructure (see [`terraform-eda-boilerplate`](https://github.com/anjalee-cooray/terraform-eda-boilerplate))
