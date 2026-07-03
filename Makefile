.PHONY: up down infra build migrate logs ps clean

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
