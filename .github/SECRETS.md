# Required GitHub Secrets

Configure these in **Settings → Secrets and variables → Actions** for each environment.

## Repository-level secrets (all environments)

| Secret | Description |
|---|---|
| `GITHUB_TOKEN` | Auto-provided by GitHub Actions — used to push images to GHCR |

## Environment secrets — `staging` and `production`

| Secret | Description | Example |
|---|---|---|
| `DB_URL` | PostgreSQL JDBC URL | `jdbc:postgresql://db.example.com:5432/edadb` |
| `DB_USERNAME` | DB username | `app_user` |
| `DB_PASSWORD` | DB password | — |
| `API_GATEWAY_URL` | Base URL for smoke test health checks | `https://api.staging.example.com` |
| `COMMAND_SERVICE_URL` | Internal command service URL | `https://command.internal.example.com` |
| `QUERY_SERVICE_URL` | Internal query service URL | `https://query.internal.example.com` |
| `OIDC_ISSUER_URI` | Okta issuer URI | `https://dev-xxx.okta.com/oauth2/default` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `broker1:9092,broker2:9092` |

## Optional secrets

| Secret | Description |
|---|---|
| `SLACK_WEBHOOK_URL` | Slack incoming webhook for deploy failure notifications |
