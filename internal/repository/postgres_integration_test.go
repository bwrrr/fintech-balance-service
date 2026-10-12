package repository

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"gor/internal/domain"
)

// Интеграционные тесты идут на настоящем Postgres и пропускаются, если
// TEST_DB_DSN не задан. Пример:
//
//	TEST_DB_DSN='postgres://user:password@localhost:5432/fintech_db?sslmode=disable' go test ./internal/repository/
//
// Тесты используют уникальные user_id и чистят за собой, чужие данные не трогают.

const schema = `CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT UNIQUE NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0)
)`

var idCounter = atomic.Int64{}

func init() { idCounter.Store(time.Now().UnixNano() % 1_000_000_000 * 1000) }

func newRepo(t *testing.T) (*PostgresRepo, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		t.Skip("TEST_DB_DSN is not set, skipping integration test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Лимит соединений Postgres по умолчанию 100, а в тестах горутин больше.
	db.SetMaxOpenConns(20)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	return NewPostgresRepo(db), db
}

func seed(t *testing.T, db *sql.DB, balance int64) int64 {
	t.Helper()
	id := idCounter.Add(1)
	if _, err := db.Exec("INSERT INTO accounts (user_id, balance) VALUES ($1, $2)", id, balance); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DELETE FROM accounts WHERE user_id = $1", id) })
	return id
}

func balance(t *testing.T, db *sql.DB, id int64) int64 {
	t.Helper()
	var b int64
	if err := db.QueryRow("SELECT balance FROM accounts WHERE user_id = $1", id).Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}

func req(from, to, amount int64) domain.TransferRequest {
	return domain.TransferRequest{IdempotencyKey: "test", FromUserID: from, ToUserID: to, Amount: amount}
}

func TestTransferSuccess(t *testing.T) {
	repo, db := newRepo(t)
	a, b := seed(t, db, 1000), seed(t, db, 50)

	if err := repo.Transfer(context.Background(), req(a, b, 300)); err != nil {
		t.Fatal(err)
	}
	if got := balance(t, db, a); got != 700 {
		t.Fatalf("sender balance = %d, want 700", got)
	}
	if got := balance(t, db, b); got != 350 {
		t.Fatalf("recipient balance = %d, want 350", got)
	}
}

func TestTransferInsufficientFunds(t *testing.T) {
	repo, db := newRepo(t)
	a, b := seed(t, db, 100), seed(t, db, 0)

	err := repo.Transfer(context.Background(), req(a, b, 101))
	if !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}
	if balance(t, db, a) != 100 || balance(t, db, b) != 0 {
		t.Fatal("balances must not change after a failed transfer")
	}
}

// Регрессия: раньше amount = -100 списывал деньги у получателя и отдавал отправителю.
func TestTransferRejectsNonPositiveAmount(t *testing.T) {
	repo, db := newRepo(t)
	a, b := seed(t, db, 100), seed(t, db, 100)

	for _, amount := range []int64{-100, 0} {
		err := repo.Transfer(context.Background(), req(a, b, amount))
		if !errors.Is(err, domain.ErrInvalidRequest) {
			t.Fatalf("amount %d: err = %v, want ErrInvalidRequest", amount, err)
		}
	}
	if balance(t, db, a) != 100 || balance(t, db, b) != 100 {
		t.Fatal("balances must not change")
	}
}

// Регрессия: раньше при несуществующем получателе деньги списывались и пропадали.
func TestTransferMissingRecipientDoesNotLoseMoney(t *testing.T) {
	repo, db := newRepo(t)
	a := seed(t, db, 100)

	err := repo.Transfer(context.Background(), req(a, a+999_999, 40))
	if !errors.Is(err, domain.ErrAccountNotFound) {
		t.Fatalf("err = %v, want ErrAccountNotFound", err)
	}
	if got := balance(t, db, a); got != 100 {
		t.Fatalf("sender balance = %d, want 100: money disappeared", got)
	}
}

func TestTransferMissingSender(t *testing.T) {
	repo, db := newRepo(t)
	b := seed(t, db, 100)

	err := repo.Transfer(context.Background(), req(b+999_999, b, 10))
	if !errors.Is(err, domain.ErrAccountNotFound) {
		t.Fatalf("err = %v, want ErrAccountNotFound", err)
	}
	if got := balance(t, db, b); got != 100 {
		t.Fatalf("recipient balance = %d, want 100", got)
	}
}

func TestTransferToSelfRejected(t *testing.T) {
	repo, db := newRepo(t)
	a := seed(t, db, 100)
	if err := repo.Transfer(context.Background(), req(a, a, 10)); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("err = %v, want ErrInvalidRequest", err)
	}
}

// 50 параллельных переводов по 100 с баланса 1000: ровно 10 должны пройти.
func TestConcurrentTransfersCannotOverdraft(t *testing.T) {
	repo, db := newRepo(t)
	a, b := seed(t, db, 1000), seed(t, db, 0)

	const workers = 50
	var ok, insufficient, other atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := repo.Transfer(context.Background(), req(a, b, 100)); {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, domain.ErrInsufficientFunds):
				insufficient.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
				other.Add(1)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != 10 || insufficient.Load() != 40 {
		t.Fatalf("ok = %d, insufficient = %d, want 10 and 40", ok.Load(), insufficient.Load())
	}
	if balance(t, db, a) != 0 || balance(t, db, b) != 1000 {
		t.Fatalf("balances = %d / %d, want 0 / 1000", balance(t, db, a), balance(t, db, b))
	}
}

// Встречные переводы A->B и B->A одновременно. Без упорядоченных блокировок
// Postgres рано или поздно убивает одну из транзакций с deadlock detected.
func TestOppositeTransfersDoNotDeadlock(t *testing.T) {
	repo, db := newRepo(t)
	a, b := seed(t, db, 100_000), seed(t, db, 100_000)

	const pairs = 100
	var wg sync.WaitGroup
	for i := 0; i < pairs; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := repo.Transfer(context.Background(), req(a, b, 10)); err != nil {
				t.Errorf("a->b: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := repo.Transfer(context.Background(), req(b, a, 10)); err != nil {
				t.Errorf("b->a: %v", err)
			}
		}()
	}
	wg.Wait()

	if balance(t, db, a) != 100_000 || balance(t, db, b) != 100_000 {
		t.Fatalf("balances = %d / %d, want 100000 / 100000", balance(t, db, a), balance(t, db, b))
	}
}
