package service

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type IdempotencyService struct {
	rdb *redis.Client
}

func NewIdempotencyService(rdb *redis.Client) *IdempotencyService {
	return &IdempotencyService{rdb: rdb}
}

func (s *IdempotencyService) LockKey(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := s.rdb.SetNX(ctx, "idemp:"+key, "locked", ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}
