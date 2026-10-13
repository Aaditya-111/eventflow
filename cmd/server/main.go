package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/Aaditya-111/eventflow/internal/handler"
	"github.com/Aaditya-111/eventflow/internal/publisher"
	"github.com/Aaditya-111/eventflow/internal/repository"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL must be set")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	startupCtx, startupCancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer startupCancel()

	if err := db.PingContext(startupCtx); err != nil {
		log.Fatalf("database unavailable: %v", err)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(startupCtx).Err(); err != nil {
		log.Fatalf("Redis unavailable: %v", err)
	}

	eventRepo := repository.NewEventRepository(db)
	outboxPublisher := publisher.NewOutboxPublisher(db, redisClient)

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	go runOutboxPublisher(workerCtx, outboxPublisher)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handler.HealthHandler)
	mux.HandleFunc("/v1/events", handler.CreateEventHandler(eventRepo))

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("EventFlow server listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	stopCtx, stopSignal := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stopSignal()

	select {
	case <-stopCtx.Done():
		log.Println("shutdown signal received")
	case err := <-serverErr:
		log.Printf("HTTP server failed: %v", err)
	}

	stopWorkers()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful HTTP shutdown failed: %v", err)
	}
}

func runOutboxPublisher(
	ctx context.Context,
	outboxPublisher *publisher.OutboxPublisher,
) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		processed, err := outboxPublisher.PublishOne(ctx)
		if err != nil {
			log.Printf("outbox publisher: %v", err)
		}

		if processed {
			continue
		}

		select {
		case <-ctx.Done():
			log.Println("outbox publisher stopped")
			return
		case <-ticker.C:
		}
	}
}
