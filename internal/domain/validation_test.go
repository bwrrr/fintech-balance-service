package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestTransferRequestValidate(t *testing.T) {
	valid := TransferRequest{IdempotencyKey: "k-1", FromUserID: 1, ToUserID: 2, Amount: 100}

	tests := []struct {
		name    string
		mutate  func(r *TransferRequest)
		wantErr bool
	}{
		{"valid", func(r *TransferRequest) {}, false},
		{"negative amount", func(r *TransferRequest) { r.Amount = -100 }, true},
		{"zero amount", func(r *TransferRequest) { r.Amount = 0 }, true},
		{"same account", func(r *TransferRequest) { r.ToUserID = r.FromUserID }, true},
		{"zero sender", func(r *TransferRequest) { r.FromUserID = 0 }, true},
		{"negative recipient", func(r *TransferRequest) { r.ToUserID = -5 }, true},
		{"empty key", func(r *TransferRequest) { r.IdempotencyKey = "" }, true},
		{"blank key", func(r *TransferRequest) { r.IdempotencyKey = "   " }, true},
		{"too long key", func(r *TransferRequest) { r.IdempotencyKey = strings.Repeat("a", MaxIdempotencyKeyLen+1) }, true},
		{"max length key", func(r *TransferRequest) { r.IdempotencyKey = strings.Repeat("a", MaxIdempotencyKeyLen) }, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.mutate(&req)
			err := req.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("error %v does not wrap ErrInvalidRequest", err)
			}
		})
	}
}

func TestValidateTransferIgnoresKey(t *testing.T) {
	// Репозиторий не знает про ключ идемпотентности, поэтому проверка счетов и суммы
	// не должна требовать ключ.
	req := TransferRequest{FromUserID: 1, ToUserID: 2, Amount: 10}
	if err := req.ValidateTransfer(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
