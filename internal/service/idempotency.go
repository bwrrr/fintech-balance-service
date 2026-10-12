package service

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyState — что мы узнали о ключе идемпотентности.
type KeyState int

const (
	// KeyAcquired — ключ наш, перевод можно выполнять.
	KeyAcquired KeyState = iota
	// KeyInProgress — другой запрос с этим ключом выполняется прямо сейчас.
	KeyInProgress
	// KeyDone — перевод по этому ключу уже выполнен.
	KeyDone
)

const (
	keyPrefix        = "idemp:"
	statusProcessing = "processing"
	statusDone       = "done"

	// Пока идёт перевод, ключ живёт недолго: если сервис упадёт, клиент не будет
	// ждать десять минут.
	processingTTL = time.Minute
	// Выполненный перевод помним сутки, чтобы поймать повтор от клиента.
	doneTTL = 24 * time.Hour
)

type IdempotencyService struct {
	rdb *redis.Client
}

func NewIdempotencyService(rdb *redis.Client) *IdempotencyService {
	return &IdempotencyService{rdb: rdb}
}

// Begin пытается занять ключ. Если ключ уже есть, различает "выполняется" и "выполнен".
func (s *IdempotencyService) Begin(ctx context.Context, key string) (KeyState, error) {
	k := keyPrefix + key

	// Две попытки: ключ мог истечь между SetNX и Get.
	for i := 0; i < 2; i++ {
		ok, err := s.rdb.SetNX(ctx, k, statusProcessing, processingTTL).Result()
		if err != nil {
			return 0, err
		}
		if ok {
			return KeyAcquired, nil
		}

		val, err := s.rdb.Get(ctx, k).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if val == statusDone {
			return KeyDone, nil
		}
		return KeyInProgress, nil
	}
	return KeyInProgress, nil
}

// Complete помечает перевод выполненным.
func (s *IdempotencyService) Complete(ctx context.Context, key string) error {
	return s.rdb.Set(ctx, keyPrefix+key, statusDone, doneTTL).Err()
}

// Release снимает ключ, когда перевод точно не выполнился и клиент может повторить запрос.
func (s *IdempotencyService) Release(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, keyPrefix+key).Err()
}
