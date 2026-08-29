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

func (r *PostgresRepo) Transfer(ctx context.Context, req domain.TransferRequest) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - $1 WHERE user_id = $2 AND balance >= $1", req.Amount, req.FromUserID)
	if err != nil {
		return err
	}

	val, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if val == 0 {
		return fmt.Errorf("insufficient funds or sender not found")
	}

	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + $1 WHERE user_id = $2", req.Amount, req.ToUserID)
	if err != nil {
		return err
	}

	return tx.Commit()
}
