# 💸 Fintech Balance & Transfer Service

Production-ready микросервис для безопасного перевода средств между счетами пользователей с защитой от состояния гонки и дублирования запросов.

## 🛠 Стек технологий
- **Go 1.22+** (`net/http`, `log/slog`, Clean Architecture)
- **PostgreSQL** (ACID транзакции, `CHECK`-ограничения на неотрицательный баланс)
- **Redis** (идемпотентность запросов через `SetNX`)
- **Apache Kafka** (публикация событий)
- **Docker Compose**

## 🚀 Запуск проекта

1. Поднимите PostgreSQL и Redis:
\`\`\`bash
docker compose up -d
\`\`\`

2. Установите зависимости и запустите API:
\`\`\`bash
go mod tidy
go run cmd/api/main.go
\`\`\`

3. Откройте веб-интерфейс в браузере:
\`\`\`text
http://localhost:8080
\`\`\`
