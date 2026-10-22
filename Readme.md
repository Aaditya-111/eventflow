# EventFlow

**A secure, asynchronous webhook delivery backend built with Go.**

EventFlow accepts application events through an authenticated HTTP API, persists them in PostgreSQL, publishes them through a transactional outbox to Redis Streams, and delivers matching events to registered webhook endpoints.

## Features

- **Authenticated APIs:** Separate bearer tokens for event ingestion and subscription creation.
- **Reliable event publishing:** PostgreSQL persistence and an outbox publisher feeding Redis Streams.
- **Asynchronous delivery:** Background workers match events to subscriptions and send webhooks.
- **Retry and failure handling:** Failed event processing and webhook delivery can be retried; exhausted event-processing retries are routed to a dead-letter stream.
- **Delivery coordination:** PostgreSQL-backed delivery records and leases support worker coordination.
- **Webhook security:** Endpoint validation, protected outbound HTTP requests, TLS configuration, and encrypted storage of subscription signing secrets.
- **Input validation:** JSON validation, request-size limits, and request timeouts.
- **Graceful shutdown:** HTTP server and background workers are shut down with a bounded timeout.

## Architecture

```text
Client
  |
  v
Authenticated Go HTTP API
  |
  v
PostgreSQL: Events + Outbox
  |
  v
Outbox Publisher
  |
  v
Redis Streams
  |
  v
Event Consumer
  |
  v
Matching Webhook Subscriptions
  |
  v
PostgreSQL: Delivery Records
  |
  v
Webhook Delivery Worker
  |
  v
Customer Webhook Endpoint
```

**Technology stack:** Go, PostgreSQL 17, Redis 7, HTTP APIs, Redis Streams, and Docker Compose for local infrastructure.

## Prerequisites

- Go 1.26 or a compatible version supporting the project's `go.mod`.
- Docker Desktop with Docker Compose.
- PowerShell on Windows, or equivalent commands for your operating system.

## Local setup

### 1. Configure environment variables

Copy the example file:

```powershell
Copy-Item .\.env.example .\.env
```

Replace every placeholder with an appropriate local value. Do not commit `.env`.

Required variables:

| Variable | Purpose |
|---|---|
| `POSTGRES_PASSWORD` | Local PostgreSQL password |
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_PASSWORD` | Redis authentication password |
| `REDIS_ADDR` | Redis address, normally `127.0.0.1:6379` |
| `WEBHOOK_ENCRYPTION_KEY` | Base64-encoded 32-byte encryption key |
| `SUBSCRIPTION_API_TOKEN` | Bearer token for subscription creation |
| `EVENT_INGESTION_API_TOKEN` | Bearer token for event ingestion |

Generate separate random tokens and an encryption key. The encryption key must decode from Base64 to exactly 32 bytes. Keep it stable: changing it without a migration or re-encryption procedure can make previously encrypted subscription secrets unreadable.

The PostgreSQL username and database name in the example are `eventflow`.

### 2. Start PostgreSQL and Redis

Load the environment variables into the current PowerShell session and start the infrastructure:

```powershell
. .\scripts\session.ps1
docker compose up -d
docker compose ps
```

Wait until both services report healthy.

### 3. Apply database migrations

For a **fresh database only**, apply the SQL migrations in numerical order. Confirm that a migration has not already been applied before executing it.

```powershell
Get-Content .\migrations\001_create_events.sql |
  docker exec -i eventflow-postgres psql -U eventflow -d eventflow

Get-Content .\migrations\002_create_outbox.sql |
  docker exec -i eventflow-postgres psql -U eventflow -d eventflow

Get-Content .\migrations\003_create_webhook_subscriptions.sql |
  docker exec -i eventflow-postgres psql -U eventflow -d eventflow

Get-Content .\migrations\004_create_webhook_deliveries.sql |
  docker exec -i eventflow-postgres psql -U eventflow -d eventflow

Get-Content .\migrations\005_add_delivery_leases.sql |
  docker exec -i eventflow-postgres psql -U eventflow -d eventflow
```

These commands apply SQL directly and do not maintain a migration-history table. Do not rerun them blindly against a database that already contains the schema.

### 4. Build and start EventFlow

```powershell
go mod download
go vet ./...
go build -o eventflow.exe ./cmd/server
.\eventflow.exe
```

The server listens on port `8080`. Keep the terminal running while testing the API.

### 5. Check service health

```powershell
Invoke-RestMethod http://localhost:8080/health
```

Expected response:

```text
EventFlow is healthy
```

The health endpoint is intended as a basic application liveness check; it does not by itself establish that every dependency is healthy.

## API usage

### Create a webhook subscription

`POST /v1/subscriptions`

Authentication:

```http
Authorization: Bearer YOUR_SUBSCRIPTION_API_TOKEN
Content-Type: application/json
```

Example request:

```json
{
  "subscription_id": "orders-demo",
  "endpoint_url": "https://your-controlled-https-endpoint.example/webhook",
  "event_types": ["order.created"],
  "secret": "replace-with-a-random-secret-of-at-least-32-characters"
}
```

Replace the example endpoint with a real HTTPS endpoint you control. Use a randomly generated signing secret and keep it private.

A successful request returns HTTP `201 Created`. Invalid input, unsafe destinations, and service errors return appropriate error responses.

### Ingest an event

`POST /v1/events`

Authentication:

```http
Authorization: Bearer YOUR_EVENT_INGESTION_API_TOKEN
Content-Type: application/json
```

Example request:

```json
{
  "event_id": "evt-order-001",
  "type": "order.created",
  "data": {
    "order_id": "order-001",
    "amount": 100,
    "currency": "INR"
  }
}
```

PowerShell example:

```powershell
. .\scripts\session.ps1

$headers = @{
    Authorization = "Bearer $env:EVENT_INGESTION_API_TOKEN"
}

$body = @{
    event_id = "evt-order-001"
    type     = "order.created"
    data     = @{
        order_id = "order-001"
        amount   = 100
        currency = "INR"
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Uri "http://localhost:8080/v1/events" `
    -Method Post `
    -Headers $headers `
    -ContentType "application/json" `
    -Body $body
```

Use a unique event ID for each new event. Reusing an existing ID may result in a conflict rather than creating another event.

### Verify delivery

Inspect the `webhook_deliveries` table for the event and subscription. A successful delivery is indicated by `status = delivered`, HTTP status `200` in `last_status_code`, and a populated `delivered_at` timestamp.

The endpoint's response and the application's logs can also help diagnose failures.

## Reliability semantics

EventFlow separates accepting an event from delivering its webhook. The outbox publisher, Redis consumer, and delivery worker operate asynchronously.

Delivery is **at-least-once**, not exactly-once. A webhook receiver should deduplicate events using the event ID because retries or failures around acknowledgement can result in duplicate requests.

Failed deliveries are scheduled for retry, and event processing has a dead-letter path. The precise retry schedule and terminal states should be verified against the implementation before using them as an operational contract.

## Security considerations

- Never commit `.env`, API tokens, database credentials, encryption keys, or real webhook secrets.
- Use HTTPS endpoints you control.
- Keep API tokens and webhook signing secrets separate.
- Preserve the webhook encryption key when restarting or redeploying.
- Use synthetic data for development and demos.
- Configure TLS termination, network access, monitoring, and production secrets management before public deployment.

## Testing and validation

Run static analysis and build validation:

```powershell
go vet ./...
go build -o eventflow.exe ./cmd/server
```

Run automated tests where your environment permits:

```powershell
go test ./...
```

On Windows, application-control policy may block execution of temporary Go test binaries. If that happens, record the failure accurately rather than treating the tests as passed. A successful build and manual end-to-end delivery test do not replace a fully passing automated test suite.

## Current project status

The local end-to-end test has demonstrated authenticated event ingestion, persistence, outbox publishing, subscription matching, and successful HTTP webhook delivery.

Before production deployment, verify automated tests, migration repeatability, operational metrics, authentication-key rotation, and deployment configuration.

## License

Add a license before distributing the project publicly.
