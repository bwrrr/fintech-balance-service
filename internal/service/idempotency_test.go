package service

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Тест идёт на настоящем Redis и пропускается без TEST_REDIS_ADDR. Пример:
//
//	TEST_REDIS_ADDR=localhost:6379 go test ./internal/service/
func newTestService(t *testing.T) (*IdempotencyService, *redis.Client) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is not set, skipping integration test")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis is unreachable: %v", err)
	}
	return NewIdempotencyService(rdb), rdb
}

func uniqueKey(t *testing.T, rdb *redis.Client) string {
	t.Helper()
	key := t.Name() + "-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { rdb.Del(context.Background(), keyPrefix+key) })
	return key
}

func TestBeginCompleteLifecycle(t *testing.T) {
	s, rdb := newTestService(t)
	ctx := context.Background()
	key := uniqueKey(t, rdb)

	if st, err := s.Begin(ctx, key); err != nil || st != KeyAcquired {
		t.Fatalf("first Begin = %v, %v, want KeyAcquired", st, err)
	}
	if st, err := s.Begin(ctx, key); err != nil || st != KeyInProgress {
		t.Fatalf("second Begin = %v, %v, want KeyInProgress", st, err)
	}
	if err := s.Complete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if st, err := s.Begin(ctx, key); err != nil || st != KeyDone {
		t.Fatalf("Begin after Complete = %v, %v, want KeyDone", st, err)
	}
}

func TestReleaseAllowsRetry(t *testing.T) {
	s, rdb := newTestService(t)
	ctx := context.Background()
	key := uniqueKey(t, rdb)

	if _, err := s.Begin(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, key); err != nil {
		t.Fatal(err)
	}
	if st, err := s.Begin(ctx, key); err != nil || st != KeyAcquired {
		t.Fatalf("Begin after Release = %v, %v, want KeyAcquired", st, err)
	}
}

func TestProcessingKeyHasShortTTLAndDoneKeyLongTTL(t *testing.T) {
	s, rdb := newTestService(t)
	ctx := context.Background()
	key := uniqueKey(t, rdb)

	s.Begin(ctx, key)
	if ttl := rdb.TTL(ctx, keyPrefix+key).Val(); ttl <= 0 || ttl > processingTTL {
		t.Fatalf("processing TTL = %v, want (0, %v]", ttl, processingTTL)
	}
	s.Complete(ctx, key)
	if ttl := rdb.TTL(ctx, keyPrefix+key).Val(); ttl <= time.Hour || ttl > doneTTL {
		t.Fatalf("done TTL = %v, want (1h, %v]", ttl, doneTTL)
	}
}

// Двадцать одновременных запросов с одним ключом: ключ должен достаться ровно одному.
func TestOnlyOneConcurrentCallerAcquiresKey(t *testing.T) {
	s, rdb := newTestService(t)
	ctx := context.Background()
	key := uniqueKey(t, rdb)

	var acquired atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if st, err := s.Begin(ctx, key); err == nil && st == KeyAcquired {
				acquired.Add(1)
			}
		}()
	}
	wg.Wait()

	if acquired.Load() != 1 {
		t.Fatalf("%d callers acquired the key, want exactly 1", acquired.Load())
	}
}
