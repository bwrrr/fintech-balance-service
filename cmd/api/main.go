package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gor/internal/handler"
	"gor/internal/repository"
	"gor/internal/service"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	dbDSN := os.Getenv("DB_DSN")
	if dbDSN == "" {
		dbDSN = "postgres://user:password@localhost:5432/fintech_db?sslmode=disable"
	}

	db, err := sql.Open("postgres", dbDSN)
	if err != nil {
		slog.Error("failed to connect postgres", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer db.Close()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	var kafkaWriter *kafka.Writer
	if kafkaBroker != "" {
		kafkaWriter = &kafka.Writer{
			Addr:     kafka.TCP(kafkaBroker),
			Topic:    "transfers",
			Balancer: &kafka.LeastBytes{},
		}
		defer kafkaWriter.Close()
	}

	repo := repository.NewPostgresRepo(db)
	idemp := service.NewIdempotencyService(rdb)
	prod := service.NewKafkaProducer(kafkaWriter)
	h := handler.NewHandler(repo, prod, idemp)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/transfer", h.TransferHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		slog.Info("server started on port :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server crashed", slog.String("err", err.Error()))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown failed", slog.String("err", err.Error()))
	}
	slog.Info("server exited cleanly")
}
