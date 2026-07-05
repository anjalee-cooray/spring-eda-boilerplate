.PHONY: up down infra build migrate logs ps clean new-schema integration-test rebuild set-quota

# Start the full local stack
up:
	docker compose up -d

# Start infrastructure only (no app services)
infra:
	docker compose up -d postgres redis kafka mock-oidc loki tempo prometheus grafana

# Build all service images
build:
	./gradlew build -x test
	docker compose build

# Run migrations only
migrate:
	docker compose run --rm db-migrations

# Tail logs for all services
logs:
	docker compose logs -f

# Show running containers
ps:
	docker compose ps

# Stop and remove containers (keeps volumes)
down:
	docker compose down

# Stop and remove containers + volumes (full reset)
clean:
	docker compose down -v

# Trigger a full tenant replay (rebuilds all read models from outbox history)
# Usage: make replay TENANT=tenant-1
replay:
	curl -s -X POST http://localhost:8084/replay/jobs \
	  -H "Content-Type: application/json" \
	  -d '{"tenantId":"$(TENANT)","requestedBy":"make-target"}' | jq

# Poll replay job status
# Usage: make replay-status JOB=<uuid>
replay-status:
	curl -s http://localhost:8084/replay/jobs/$(JOB) | jq

# List all replay jobs for a tenant
# Usage: make replay-list TENANT=tenant-1
replay-list:
	curl -s "http://localhost:8084/replay/jobs?tenantId=$(TENANT)" | jq

# Start LocalStack for SNS/SQS local development
infra-sns:
	docker compose --profile sns up localstack -d

# Create SNS topics and SQS queues + DLQs in LocalStack
# Run once after: make infra-sns
sns-setup:
	@echo "Creating SQS queues..."
	aws --endpoint-url=http://localhost:4566 --region us-east-1 \
	    sqs create-queue --queue-name example-created-dlq \
	    --attributes '{"MessageRetentionPeriod":"1209600"}'
	aws --endpoint-url=http://localhost:4566 --region us-east-1 \
	    sqs create-queue --queue-name example-created \
	    --attributes '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:example-created-dlq\",\"maxReceiveCount\":\"3\"}"}'
	@echo "Creating SNS topic..."
	aws --endpoint-url=http://localhost:4566 --region us-east-1 \
	    sns create-topic --name example-created
	@echo "Subscribing SQS queue to SNS topic..."
	aws --endpoint-url=http://localhost:4566 --region us-east-1 \
	    sns subscribe \
	    --topic-arn arn:aws:sns:us-east-1:000000000000:example-created \
	    --protocol sqs \
	    --notification-endpoint arn:aws:sqs:us-east-1:000000000000:example-created
	@echo "SNS/SQS setup complete. Queue URL: http://localhost:4566/000000000000/example-created"

# ── Schema versioning ─────────────────────────────────────────────────────────
# Scaffold a new event schema version JSON file and upcaster Java stub.
# Usage: make new-schema EVENT_TYPE=example.created VERSION=2 SERVICE=example-consumer-service
new-schema:
	@if [ -z "$(EVENT_TYPE)" ] || [ -z "$(VERSION)" ] || [ -z "$(SERVICE)" ]; then \
	  echo "Usage: make new-schema EVENT_TYPE=<type> VERSION=<N> SERVICE=<module>"; \
	  echo "  Example: make new-schema EVENT_TYPE=example.created VERSION=2 SERVICE=example-consumer-service"; \
	  exit 1; \
	fi
	./scripts/new-schema.sh --event-type $(EVENT_TYPE) --version $(VERSION) --service $(SERVICE)

# ── Integration tests ─────────────────────────────────────────────────────────
# Run all @Tag("integration") tests via Testcontainers.
# Requires Docker running. First run pulls container images (~1-2 min).
integration-test:
	./gradlew integrationTest

# ── Tenant quota management ──────────────────────────────────────────────────
# Apply a tier preset for a tenant. Tier: FREE (1k/day) | PRO (10k/day) | ENTERPRISE (unlimited)
# Usage: make set-quota TENANT=tenant-1 TIER=PRO
set-quota:
	curl -s -X PUT http://localhost:9080/admin/quotas/$(TENANT)/tier/$(TIER) \
	  -H "Authorization: Bearer $${JWT_TOKEN}" | jq

# ── Read model rebuild ────────────────────────────────────────────────────────
# Trigger a read-model rebuild for a specific projection and tenant.
# Usage: make rebuild PROJECTION=ExampleProjector VERSION=2 TENANT=tenant-1
rebuild:
	curl -s -X POST http://localhost:8082/projections/$(PROJECTION)/rebuild \
	  -H "Content-Type: application/json" \
	  -H "Authorization: Bearer $${JWT_TOKEN}" \
	  -d '{"targetVersion":$(VERSION),"requestedBy":"make-target"}' | jq
