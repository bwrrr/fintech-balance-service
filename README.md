Fintech Balance and Transfer Service

A microservice built with Go for secure and reliable money transfers between user accounts, featuring race condition prevention and duplicate request protection.

Tech Stack
- Go (net/http, log/slog)
- Clean Architecture (Domain, Repository, Service, Handler)
- PostgreSQL (ACID transactions, non-negative balance checks)
- Redis (Idempotency keys via SetNX)
- Apache Kafka (Asynchronous event publishing)
- Docker and Docker Compose

Key Features
- Atomic balance transfers inside a single database transaction
- Idempotency guarantee via Redis to prevent double spending
- Overdraft protection at both query and database levels
- Async event publishing to Kafka upon completed transactions
- Simple web client for testing

Quick Start

1. Clone the repository:
git clone https://github.com/bwrrr/fintech-balance-service.git
cd fintech-balance-service

2. Start PostgreSQL and Redis:
docker compose up -d

3. Download dependencies and run the server:
go mod tidy
go run cmd/api/main.go

4. Open the web interface:
http://localhost:8080

API Reference

Endpoint: POST /api/transfer
Content-Type: application/json

Request Body:
{
  "idempotency_key": "unique-uuid-12345",
  "from_user_id": 1,
  "to_user_id": 2,
  "amount": 500
}

Responses:
- 200 OK: Transfer completed successfully
- 400 Bad Request: Invalid payload or insufficient funds
- 409 Conflict: Duplicate request (idempotency key already used)
- 500 Internal Server Error: Database or cache failure
