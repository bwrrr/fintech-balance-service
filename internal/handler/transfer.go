package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"gor/internal/domain"
	"gor/internal/repository"
	"gor/internal/service"
)

type Handler struct {
	repo        *repository.PostgresRepo
	producer    *service.KafkaProducer
	idempotency *service.IdempotencyService
}

func NewHandler(repo *repository.PostgresRepo, producer *service.KafkaProducer, idempotency *service.IdempotencyService) *Handler {
	return &Handler{
		repo:        repo,
		producer:    producer,
		idempotency: idempotency,
	}
}

func (h *Handler) TransferHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req domain.TransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	locked, err := h.idempotency.LockKey(r.Context(), req.IdempotencyKey, 10*time.Minute)
	if err != nil {
		http.Error(w, "internal cache error", http.StatusInternalServerError)
		return
	}
	if !locked {
		http.Error(w, "duplicate request: idempotency key already used", http.StatusConflict)
		return
	}

	if err := h.repo.Transfer(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.producer.SendTransferEvent(r.Context(), req); err != nil {
		slog.Error("failed to publish kafka event", slog.String("err", err.Error()))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success","message":"transfer completed"}`))
}
