# spring-eda-boilerplate

Production-ready boilerplate for building multi-tenant, event-driven SaaS backends on Java 25 and Spring Boot. Ships with multi-tenancy, CQRS, the Transactional Outbox pattern, idempotent consumers, saga orchestration, pluggable messaging (Kafka or SNS/SQS), pluggable auth (any OIDC provider), pluggable payments (Stripe), and a full Grafana LGTM observability stack — all wired together and ready to adopt.

See [`next-eda-boilerplate`](https://github.com/anjalee-cooray/next-eda-boilerplate) for the frontend counterpart and [`terraform-eda-boilerplate`](https://github.com/anjalee-cooray/terraform-eda-boilerplate) for cloud infrastructure.

---

## What's included

| Category | Detail |
|---|---|
| **Language** | Java 25 with Project Loom (virtual threads via `--enable-preview`) |
| **Framework** | Spring Boot 3.4.1, Spring Cloud Gateway, Spring Security OAuth2 |
| **Multi-tenancy** | PostgreSQL Row-Level Security, `SET LOCAL app.tenant_id` per request |
| **Async messaging** | Transactional Outbox pattern; pluggable Kafka or SNS/SQS |
| **Data consistency** | Idempotent consumers via inbox deduplication table |
| **Architecture** | CQRS (separate command + query services), event choreography pattern |
| **Auth** | OIDC JWT — Okta by default, any OIDC-compliant provider via one env var |
| **Payments** | `PaymentGateway` interface, Stripe implementation |
| **Observability** | OTel traces → Tempo, structured logs → Loki, metrics → Prometheus, Grafana dashboards |
| **CI/CD** | GitHub Actions: Checkstyle, unit tests, integration tests (Testcontainers), GHCR image build, rolling deploy with smoke tests and rollback |
| **Build** | Gradle multi-module monorepo, `buildSrc` conventions |

---

## Architecture

```
                        ┌────────────┐
   Browser / Client ──► │ api-gateway │ (JWT validation, rate limiting, routing)
                        └──────┬─────┘
                               │ HTTP
               ┌───────────────┴───────────────┐
               ▼                               ▼
  ┌────────────────────┐           ┌────────────────────┐
  │  command-service   │           │   query-service     │
  │  CQRS write side   │           │   CQRS read side    │
  │  (domain writes,   │           │   (read model,      │
  │   outbox)          │           │    projections)     │
  └────────┬───────────┘           └──────────┬─────────┘
           │ outbox_records                    │ subscribes
           ▼                                  │
  ┌────────────────┐     ┌───────────────────────────────┐
  │  outbox-relay  │────►│      Kafka / SNS+SQS           │
  └────────────────┘     └───────────────────────────────┘
                                               │ subscribes
                                               ▼
                                  ┌────────────────────────┐
                                  │   consumer-service      │
                                  │   Event-Driven Pub/Sub  │
                                  │   (notifications,       │
                                  │    billing, audit, etc) │
                                  └────────────────────────┘

  ┌────────────────┐
  │  db-migrations │  short-lived Flyway job — runs before services start
  └────────────────┘
```

---

## Project structure

```
spring-eda-boilerplate/
├── gradle/buildSrc/           # Shared Gradle conventions
│   └── src/main/groovy/
│       ├── java-conventions.gradle      # Java 25, Checkstyle, integrationTest source set
│       └── spring-conventions.gradle   # Extends java-conventions, adds Spring Boot
├── shared/
│   ├── shared-logging/        # MDC filter, Logback JSON config, Loki appender
│   ├── shared-telemetry/      # OTel, Micrometer, virtual thread metrics
│   ├── shared-security/       # TenantContext, OIDC JWT filter, roles
│   ├── shared-db/             # RLS interceptor, OutboxWriter, InboxDeduplicator
│   ├── shared-events/         # EventEnvelope, EventPublisher, Kafka + SNS/SQS
│   └── shared-payments/       # PaymentGateway interface, Stripe implementation
├── services/
│   ├── db-migrations/         # Flyway migrations only — exits after running
│   ├── api-gateway/           # Spring Cloud Gateway + JWT + tenant propagation
│   ├── example-command-service/  # Command handling, outbox write, saga skeleton
│   ├── example-query-service/    # CQRS read model, event projections
│   ├── example-consumer-service/ # Idempotent event consumer
│   └── outbox-relay/          # Outbox poller — publishes PENDING events to broker
├── docker/                    # Config files for Tempo, Prometheus, Grafana
├── config/checkstyle/         # checkstyle.xml
├── docker-compose.yml
├── Makefile
├── .env.example
├── BOILERPLATE_SPEC.md        # Full architecture reference
└── .github/
    ├── workflows/
    │   ├── pr-pipeline.yml    # Checkstyle → tests → build images on PR
    │   ├── deploy-backend.yml # Migrate → deploy → smoke test → rollback on main
    │   └── destroy.yml        # Manual env destroy (dev only)
    └── SECRETS.md             # Required GitHub Secrets reference
```

---

## Prerequisites

- Docker + Docker Compose
- Java 25 ([Temurin](https://adoptium.net/))
- Gradle wrapper is included — no separate install needed

---

## Quickstart

```bash
# 1. Clone
git clone https://github.com/anjalee-cooray/spring-eda-boilerplate
cd spring-eda-boilerplate

# 2. Configure environment
cp .env.example .env
# Default values work for local dev out of the box

# 3. Start infrastructure (Postgres, Redis, Kafka, OIDC, observability)
make infra

# 4. Build JARs
./gradlew build -x test

# 5. Build Docker images
docker compose build

# 6. Start everything
make up

# 7. Tail logs
make logs
```

Stack is up when `docker compose ps` shows all services healthy.

### Get an access token (local dev)

```bash
TOKEN=$(curl -s -X POST http://localhost:8090/okta/token \
  -d "grant_type=client_credentials&client_id=test&client_secret=test&scope=openid" \
  | jq -r '.access_token')
```

The mock OIDC server issues tokens with `tenant_id: tenant-1` and `roles: [TENANT_ADMIN]`.

### Test the happy path

```bash
# Create an entity
curl -s -X POST http://localhost:9080/api/commands/examples \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "hello"}' | jq

# Query the read model (allow a second for projection)
curl -s http://localhost:9080/api/queries/examples \
  -H "Authorization: Bearer $TOKEN" | jq
```

### Local service URLs

| Service | URL |
|---|---|
| API Gateway | http://localhost:9080 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Mock OIDC | http://localhost:8090 |

---

## Incremental adoption — start with command service only

You don't need to run all services from day one. A common growth path:

**Stage 1 — Command service only**

```bash
# Minimal infrastructure — no Kafka, no relay, no query service
docker compose up postgres redis mock-oidc prometheus grafana

./gradlew :services:example-command-service:bootRun
```

Don't set `app.events.broker` — `NoOpEventPublisher` activates automatically and logs events instead of publishing them. Outbox records accumulate as `PENDING` and are harmless.

Add GET endpoints directly to the command controller to read data. No query service, no eventual consistency to reason about.

**Stage 2** — Add `kafka` + `outbox-relay` when you need async side effects.

**Stage 3** — Add `example-query-service` when read shape diverges from write model.

**Stage 4** — Add `example-consumer-service` (rename it `notification-service`, `billing-service`, etc.) when side effects grow enough to deserve their own deployment.

---

## Makefile targets

| Target | What it does |
|---|---|
| `make infra` | Start infrastructure services only (Postgres, Redis, Kafka, observability, OIDC) |
| `make up` | Start the full stack |
| `make build` | Build all JARs + Docker images |
| `make migrate` | Run `db-migrations` against the local database |
| `make logs` | Tail all service logs |
| `make ps` | Show service status |
| `make down` | Stop all containers |
| `make clean` | Stop containers and remove volumes |

---

## Pluggable points

### Auth provider

Set `OIDC_ISSUER_URI` in `.env` — no code changes required:

```bash
# Okta
OIDC_ISSUER_URI=https://dev-xxx.okta.com/oauth2/default

# Auth0
OIDC_ISSUER_URI=https://your-tenant.auth0.com/

# Cognito
OIDC_ISSUER_URI=https://cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx
```

**Okta claim mapping** — add these via Expression Language in your Okta authorization server:
- `tenant_id` — custom claim identifying the tenant
- `roles` — custom claim mapped from Okta groups

### Messaging broker

Set `app.events.broker` in each service's `application.yml`:

```yaml
# Kafka (default for local dev)
app.events.broker: kafka

# AWS SNS/SQS
app.events.broker: sns
```

### Payment provider

```yaml
# Stripe (default)
app.payments.provider: stripe
```

Implement `PaymentGateway` and annotate with `@ConditionalOnProperty` to add a provider.

---

## Running tests

```bash
# Unit tests
./gradlew test

# Integration tests (requires Docker — Testcontainers spins up Postgres + Kafka)
./gradlew integrationTest

# Checkstyle
./gradlew checkstyleMain
```

---

## CI/CD

### PR pipeline

Every PR runs: Checkstyle → unit tests → integration tests (Testcontainers) → build and push Docker images to GHCR tagged `pr-{N}` and `sha-{SHA}`.

### Deploy pipeline

Triggered on push to `main` or manually via `workflow_dispatch`. Stages: build images → run migrations → rolling deploy (one service at a time) → smoke tests → auto-rollback if smoke test fails.

Fill in the deploy target — look for the `# Replace this step` comments in `.github/workflows/deploy-backend.yml` and swap in your ECS / Kubernetes commands.

See [`.github/SECRETS.md`](.github/SECRETS.md) for required GitHub Secrets.

---

## Adopting this boilerplate

1. Rename the Gradle group — replace `com.example.eda` with your group ID across all `build.gradle` files and Java packages
2. Delete the `example-*` domain classes and replace with your own entities, commands, and events
3. Wire your OIDC provider via `OIDC_ISSUER_URI`
4. Set your broker (`kafka` or `sns`) and add connection config
5. Add domain migrations starting at `V006__` following the existing RLS pattern
6. Replace the `echo` stubs in the deploy pipeline with your ECS / Kubernetes deploy commands
7. Optionally publish `shared-*` modules to a private Maven registry if teams split into separate repos

Full architecture deep dive (request flows, RLS, observability, local verification): [`ARCHITECTURE.md`](ARCHITECTURE.md).
Full pattern reference: [`BOILERPLATE_SPEC.md`](BOILERPLATE_SPEC.md).

---

## Tech stack

| Layer | Technology |
|---|---|
| Language | Java 25 (Project Loom) |
| Framework | Spring Boot 3.4.1 |
| Gateway | Spring Cloud Gateway |
| Auth | Spring Security OAuth2 Resource Server (OIDC) |
| Database | PostgreSQL 16 with RLS |
| Migrations | Flyway |
| Cache | Redis 7 |
| Messaging | Kafka (KRaft) or AWS SNS+SQS |
| Payments | Stripe (via `PaymentGateway` interface) |
| Traces | OpenTelemetry Java agent → Grafana Tempo |
| Logs | Logback + Loki4j → Grafana Loki |
| Metrics | Micrometer → Prometheus → Grafana |
| Build | Gradle 8, multi-module monorepo |
| Containers | Docker, GHCR |
| CI/CD | GitHub Actions |
| Local OIDC | mock-oauth2-server (Okta-compatible) |
