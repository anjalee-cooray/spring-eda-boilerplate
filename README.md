# spring-eda-boilerplate

A production-ready Spring Boot microservices starter for building multi-tenant, event-driven SaaS applications on Java 25.

It ships with the hard architectural decisions already made and wired together: PostgreSQL row-level security for tenant isolation, a transactional outbox with a dedicated relay service, idempotent event consumers, and a CQRS split between command and query services. A saga orchestration skeleton is included in the command service. Virtual threads (Project Loom) are enabled by default across all services.

Everything external is pluggable behind interfaces — swap Kafka for SNS/SQS, Okta for Auth0 or Keycloak, Stripe for another payment provider, or AWS for Azure — without touching business logic. The gateway handles OIDC JWT validation and tenant context propagation so downstream services never deal with auth plumbing directly.

## Core patterns

- Multi-tenancy — PostgreSQL RLS with safe null default
- Transactional Outbox — atomic domain write + event write
- Idempotent Consumer — inbox_records deduplication
- CQRS — separate command and query services
- Saga skeleton — orchestration hooks in command service
- Event-Carried State Transfer — EventEnvelope with full payload
- Virtual threads — Project Loom enabled by default

## Pluggable

- Messaging — Kafka or SNS/SQS via EventPublisher interface
- Auth — any OIDC provider (Okta, Auth0, Cognito, Keycloak)
- Payments — PaymentGateway interface (Stripe implementation included)
- Cloud — AWS or Azure via separate Terraform modules
