package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"gor/internal/domain"
	"gor/internal/service"
)

const (
	maxBodyBytes = 1 << 20 // 1 MiB
	// Побочные действия после перевода (Redis, Kafka) выполняем с отдельным
	// контекстом: если клиент оборвал соединение, деньги уже переведены и
	// ключ должен быть помечен.
	sideEffectTimeout = 3 * time.Second
)

// Интерфейсы объявлены здесь, где они используются, поэтому хендлер
// тестируется без Postgres, Redis и Kafka.
type TransferRepository interface {
	Transfer(ctx context.Context, req domain.TransferRequest) error
}

type EventPublisher interface {
	SendTransferEvent(ctx context.Context, req domain.TransferRequest) error
}

type IdempotencyStore interface {
	Begin(ctx context.Context, key string) (service.KeyState, error)
	Complete(ctx context.Context, key string) error
	Release(ctx context.Context, key string) error
}

type Handler struct {
	repo        TransferRepository
	producer    EventPublisher
	idempotency IdempotencyStore
}

func NewHandler(repo TransferRepository, producer EventPublisher, idempotency IdempotencyStore) *Handler {
	return &Handler{
		repo:        repo,
		producer:    producer,
		idempotency: idempotency,
	}
}

func (h *Handler) TransferHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req domain.TransferRequest
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	state, err := h.idempotency.Begin(r.Context(), req.IdempotencyKey)
	if err != nil {
		slog.Error("idempotency check failed", slog.String("err", err.Error()))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	switch state {
	case service.KeyInProgress:
		http.Error(w, "request with this idempotency key is still being processed", http.StatusConflict)
		return
	case service.KeyDone:
		http.Error(w, "duplicate request: idempotency key already used", http.StatusConflict)
		return
	}

	if err := h.repo.Transfer(r.Context(), req); err != nil {
		h.failTransfer(w, r, req, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), sideEffectTimeout)
	defer cancel()

	if err := h.idempotency.Complete(ctx, req.IdempotencyKey); err != nil {
		slog.Error("failed to mark idempotency key done", slog.String("err", err.Error()))
	}
	if err := h.producer.SendTransferEvent(ctx, req); err != nil {
		slog.Error("failed to publish kafka event", slog.String("err", err.Error()))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success","message":"transfer completed"}`))
}

func (h *Handler) failTransfer(w http.ResponseWriter, r *http.Request, req domain.TransferRequest, err error) {
	switch {
	case errors.Is(err, domain.ErrInsufficientFunds),
		errors.Is(err, domain.ErrAccountNotFound),
		errors.Is(err, domain.ErrInvalidRequest):
		// Перевод точно не выполнился, поэтому клиент вправе повторить запрос
		// с тем же ключом (например, после пополнения счёта).
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), sideEffectTimeout)
		defer cancel()
		if relErr := h.idempotency.Release(ctx, req.IdempotencyKey); relErr != nil {
			slog.Error("failed to release idempotency key", slog.String("err", relErr.Error()))
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		// Исход неизвестен (например, оборвалась связь на COMMIT). Ключ намеренно
		// не снимаем: он истечёт сам, а повтор тем временем получит 409, а не
		// рискнёт задвоить перевод. Текст ошибки БД клиенту не отдаём.
		slog.Error("transfer failed", slog.String("err", err.Error()))
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
