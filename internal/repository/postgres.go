package repository

import (
	"context"
	"database/sql"
	"fmt"

	"gor/internal/domain"
)

type PostgresRepo struct {
	db *sql.DB
}

func NewPostgresRepo(db *sql.DB) *PostgresRepo {
	return &PostgresRepo{db: db}
}

// Transfer переводит деньги атомарно в одной транзакции.
//
// Что здесь важно:
//   - сумма и счета валидируются до обращения к БД (отрицательная сумма иначе
//     обернула бы перевод в обратную сторону);
//   - обе строки блокируются через SELECT ... FOR UPDATE в порядке возрастания
//     user_id, поэтому встречные переводы A->B и B->A не уходят в дедлок;
//   - существование получателя проверяется до списания, иначе деньги
//     списались бы и пропали;
//   - условие balance >= amount в UPDATE и CHECK в таблице защищают от овердрафта.
func (r *PostgresRepo) Transfer(ctx context.Context, req domain.TransferRequest) error {
	if err := req.ValidateTransfer(); err != nil {
		return err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := lockAccounts(ctx, tx, req.FromUserID, req.ToUserID); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx,
		"UPDATE accounts SET balance = balance - $1 WHERE user_id = $2 AND balance >= $1",
		req.Amount, req.FromUserID)
	if err != nil {
		return fmt.Errorf("debit sender: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("debit rows affected: %w", err)
	} else if n == 0 {
		// Строка существует (мы её заблокировали), значит не хватает денег.
		return domain.ErrInsufficientFunds
	}

	res, err = tx.ExecContext(ctx,
		"UPDATE accounts SET balance = balance + $1 WHERE user_id = $2",
		req.Amount, req.ToUserID)
	if err != nil {
		return fmt.Errorf("credit recipient: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("credit rows affected: %w", err)
	} else if n != 1 {
		return fmt.Errorf("credit recipient: expected 1 row affected, got %d", n)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// lockAccounts блокирует обе строки в порядке возрастания user_id и проверяет,
// что оба счёта существуют.
func lockAccounts(ctx context.Context, tx *sql.Tx, a, b int64) error {
	rows, err := tx.QueryContext(ctx,
		"SELECT user_id FROM accounts WHERE user_id IN ($1, $2) ORDER BY user_id FOR UPDATE", a, b)
	if err != nil {
		return fmt.Errorf("lock accounts: %w", err)
	}
	defer rows.Close()

	found := 0
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan locked account: %w", err)
		}
		found++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("lock accounts: %w", err)
	}
	if found != 2 {
		return domain.ErrAccountNotFound
	}
	return nil
}
