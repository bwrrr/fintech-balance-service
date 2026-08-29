package domain

type Account struct {
	ID      int64 `json:"id"`
	UserID  int64 `json:"user_id"`
	Balance int64 `json:"balance"`
}

type TransferRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	FromUserID     int64  `json:"from_user_id"`
	ToUserID       int64  `json:"to_user_id"`
	Amount         int64  `json:"amount"`
}

type TransferEvent struct {
	FromUserID int64  `json:"from_user_id"`
	ToUserID   int64  `json:"to_user_id"`
	Amount     int64  `json:"amount"`
	Timestamp  string `json:"timestamp"`
}
