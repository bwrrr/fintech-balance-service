package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gor/internal/domain"
	"gor/internal/service"
)

// ---- fakes ----

type fakeRepo struct {
	err   error
	calls int
}

func (f *fakeRepo) Transfer(ctx context.Context, req domain.TransferRequest) error {
	f.calls++
	return f.err
}

type fakePublisher struct {
	err   error
	calls int
}

func (f *fakePublisher) SendTransferEvent(ctx context.Context, req domain.TransferRequest) error {
	f.calls++
	return f.err
}

type fakeIdemp struct {
	state      service.KeyState
	beginErr   error
	beginCalls int
	completed  []string
	released   []string
}

func (f *fakeIdemp) Begin(ctx context.Context, key string) (service.KeyState, error) {
	f.beginCalls++
	return f.state, f.beginErr
}
func (f *fakeIdemp) Complete(ctx context.Context, key string) error {
	f.completed = append(f.completed, key)
	return nil
}
func (f *fakeIdemp) Release(ctx context.Context, key string) error {
	f.released = append(f.released, key)
	return nil
}

// ---- helpers ----

const validBody = `{"idempotency_key":"k-1","from_user_id":1,"to_user_id":2,"amount":500}`

func do(h *Handler, method, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/api/transfer", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.TransferHandler(rec, req)
	return rec
}

func setup() (*Handler, *fakeRepo, *fakePublisher, *fakeIdemp) {
	repo, pub, idem := &fakeRepo{}, &fakePublisher{}, &fakeIdemp{state: service.KeyAcquired}
	return NewHandler(repo, pub, idem), repo, pub, idem
}

// ---- tests ----

func TestSuccess(t *testing.T) {
	h, repo, pub, idem := setup()

	rec := do(h, http.MethodPost, validBody)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if repo.calls != 1 || pub.calls != 1 {
		t.Fatalf("repo calls = %d, publisher calls = %d, want 1 and 1", repo.calls, pub.calls)
	}
	if len(idem.completed) != 1 || idem.completed[0] != "k-1" {
		t.Fatalf("key must be marked done, got %v", idem.completed)
	}
	if len(idem.released) != 0 {
		t.Fatalf("key must not be released on success, got %v", idem.released)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h, repo, _, idem := setup()
	rec := do(h, http.MethodGet, "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if repo.calls != 0 || idem.beginCalls != 0 {
		t.Fatal("nothing must be called for a wrong method")
	}
}

func TestInvalidRequestsNeverTouchStorage(t *testing.T) {
	bodies := map[string]string{
		"broken json":      `{`,
		"unknown field":    `{"idempotency_key":"k","from_user_id":1,"to_user_id":2,"amount":5,"extra":1}`,
		"negative amount":  `{"idempotency_key":"k","from_user_id":1,"to_user_id":2,"amount":-100}`,
		"zero amount":      `{"idempotency_key":"k","from_user_id":1,"to_user_id":2,"amount":0}`,
		"same account":     `{"idempotency_key":"k","from_user_id":1,"to_user_id":1,"amount":5}`,
		"missing key":      `{"from_user_id":1,"to_user_id":2,"amount":5}`,
		"string for int64": `{"idempotency_key":"k","from_user_id":"1","to_user_id":2,"amount":5}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			h, repo, pub, idem := setup()
			rec := do(h, http.MethodPost, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if repo.calls != 0 || pub.calls != 0 || idem.beginCalls != 0 {
				t.Fatalf("storage must not be touched: repo=%d pub=%d begin=%d", repo.calls, pub.calls, idem.beginCalls)
			}
		})
	}
}

func TestDuplicateRequests(t *testing.T) {
	for name, state := range map[string]service.KeyState{
		"already done":    service.KeyDone,
		"still in flight": service.KeyInProgress,
	} {
		t.Run(name, func(t *testing.T) {
			h, repo, pub, idem := setup()
			idem.state = state
			rec := do(h, http.MethodPost, validBody)
			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409", rec.Code)
			}
			if repo.calls != 0 || pub.calls != 0 {
				t.Fatal("a duplicate must not move money or publish an event")
			}
		})
	}
}

func TestBusinessErrorsReleaseKey(t *testing.T) {
	for name, repoErr := range map[string]error{
		"insufficient funds": domain.ErrInsufficientFunds,
		"account not found":  domain.ErrAccountNotFound,
		"wrapped":            fmt.Errorf("wrapped: %w", domain.ErrInsufficientFunds),
	} {
		t.Run(name, func(t *testing.T) {
			h, repo, pub, idem := setup()
			repo.err = repoErr
			rec := do(h, http.MethodPost, validBody)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if len(idem.released) != 1 {
				t.Fatalf("key must be released so the client can retry, got %v", idem.released)
			}
			if len(idem.completed) != 0 || pub.calls != 0 {
				t.Fatal("a failed transfer must not be marked done or published")
			}
		})
	}
}

func TestInternalErrorKeepsKeyAndHidesDetails(t *testing.T) {
	h, repo, pub, idem := setup()
	repo.err = errors.New(`pq: connection to "db-secret-host" lost`)

	rec := do(h, http.MethodPost, validBody)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "db-secret-host") {
		t.Fatalf("internal error text leaked to the client: %q", rec.Body.String())
	}
	if len(idem.released) != 0 {
		t.Fatal("outcome is unknown, so the key must not be released")
	}
	if pub.calls != 0 {
		t.Fatal("no event for a failed transfer")
	}
}

func TestIdempotencyStoreFailure(t *testing.T) {
	h, repo, _, idem := setup()
	idem.beginErr = errors.New("redis down")
	rec := do(h, http.MethodPost, validBody)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if repo.calls != 0 {
		t.Fatal("without an idempotency guarantee money must not move")
	}
}

func TestKafkaFailureDoesNotFailTransfer(t *testing.T) {
	h, _, pub, idem := setup()
	pub.err = errors.New("kafka down")
	rec := do(h, http.MethodPost, validBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: the money is already moved", rec.Code)
	}
	if len(idem.completed) != 1 {
		t.Fatal("key must still be marked done")
	}
}
