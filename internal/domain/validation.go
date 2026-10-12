package domain

import (
	"errors"
	"fmt"
	"strings"
)

// MaxIdempotencyKeyLen ограничивает длину ключа, чтобы в Redis не клали мусор.
const MaxIdempotencyKeyLen = 128

var (
	// ErrInvalidRequest — запрос не прошёл валидацию (клиентская ошибка, 400).
	ErrInvalidRequest = errors.New("invalid transfer request")
	// ErrInsufficientFunds — у отправителя не хватает денег.
	ErrInsufficientFunds = errors.New("insufficient funds")
	// ErrAccountNotFound — отправитель или получатель не существует.
	ErrAccountNotFound = errors.New("account not found")
)

func invalid(msg string) error {
	return fmt.Errorf("%w: %s", ErrInvalidRequest, msg)
}

// ValidateTransfer проверяет счета и сумму. Её вызывает и хендлер, и репозиторий:
// даже если кто-то вызовет репозиторий напрямую, отрицательный перевод не пройдёт.
func (r TransferRequest) ValidateTransfer() error {
	switch {
	case r.FromUserID <= 0 || r.ToUserID <= 0:
		return invalid("from_user_id and to_user_id must be positive")
	case r.FromUserID == r.ToUserID:
		return invalid("sender and recipient must differ")
	case r.Amount <= 0:
		return invalid("amount must be positive")
	}
	return nil
}

// Validate проверяет запрос целиком, включая ключ идемпотентности.
func (r TransferRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.IdempotencyKey) == "":
		return invalid("idempotency_key is required")
	case len(r.IdempotencyKey) > MaxIdempotencyKeyLen:
		return invalid("idempotency_key is too long")
	}
	return r.ValidateTransfer()
}
