package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/Aaditya-111/eventflow/internal/handler"
	"github.com/Aaditya-111/eventflow/internal/publisher"
	"github.com/Aaditya-111/eventflow/internal/repository"
	"github.com/Aaditya-111/eventflow/internal/security"
	"github.com/Aaditya-111/eventflow/internal/service"
	"github.com/Aaditya-111/eventflow/internal/worker"
)

func main() {
	// Load and initialize webhook secret encryption.
	encodedKey := os.Getenv("WEBHOOK_ENCRYPTION_KEY")
	if encodedKey == "" {
		log.Fatal("WEBHOOK_ENCRYPTION_KEY must be set")
	}

	encryptionKey, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		log.Fatal("WEBHOOK_ENCRYPTION_KEY must be valid Base64")
	}

	secretManager, err := security.NewSecretManager(encryptionKey)
	clear(encryptionKey)
	if err != nil {
		log.Fatalf("initialize webhook secret encryption: %v", err)
	}

	// Connect to PostgreSQL.
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
		context.Background(),
		5*time.Second,
	)
	defer startupCancel()

	if err := db.PingContext(startupCtx); err != nil {
		log.Fatalf("database unavailable: %v", err)
	}

	// Connect to Redis.
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

	// Initialize repositories.
	eventRepo := repository.NewEventRepository(db)
	subscriptionRepo := repository.NewSubscriptionRepository(db)
	deliveryRepo := repository.NewDeliveryRepository(db)

	// Initialize subscription service.
	subscriptionService, err := service.NewSubscriptionService(
		subscriptionRepo,
		secretManager,
	)
	if err != nil {
		log.Fatalf("initialize subscription service: %v", err)
	}

	// Initialize transactional outbox publisher.
	outboxPublisher := publisher.NewOutboxPublisher(db, redisClient)

	// Initialize Redis event consumer.
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "eventflow"
	}
	consumerName := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	consumer := worker.NewConsumer(
		redisClient,
		worker.EventConsumerGroup,
		consumerName,
	)
	processor := worker.NewProcessor(consumer)
	deadLetters := worker.NewRedisDeadLetterSink(redisClient)

	// Initialize the secure webhook HTTP client.
	webhookClient, err := security.NewWebhookHTTPClient(10 * time.Second)
	if err != nil {
		log.Fatalf("initialize webhook HTTP client: %v", err)
	}

	// Initialize webhook sender with destination validation.
	webhookSender, err := worker.NewWebhookSender(
		webhookClient,
		security.NetDNSResolver{},
	)
	if err != nil {
		log.Fatalf("initialize webhook sender: %v", err)
	}

	// Give this delivery worker a unique identity for database leases.
	deliveryWorkerID, err := worker.NewDeliveryWorkerID()
	if err != nil {
		log.Fatalf("initialize delivery worker ID: %v", err)
	}

	// Initialize the webhook delivery worker.
	deliveryWorker, err := worker.NewDeliveryWorker(
		deliveryWorkerID,
		deliveryRepo,
		eventRepo,
		subscriptionRepo,
		deliveryRepo,
		secretManager,
		webhookSender,
	)
	if err != nil {
		log.Fatalf("initialize delivery worker: %v", err)
	}

	// Connect processed events to matching webhook subscriptions.
	eventHandler, err := worker.NewDeliveryEnqueueHandler(
		subscriptionRepo,
		deliveryRepo,
	)
	if err != nil {
		log.Fatalf("initialize event delivery handler: %v", err)
	}

	runner, err := worker.NewRunner(
		consumer,
		processor,
		eventHandler,
		deadLetters,
		worker.DefaultRetryPolicy(),
	)
	if err != nil {
		log.Fatalf("initialize event worker: %v", err)
	}

	// Start all background workers under one cancellable context.
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	var workerWG sync.WaitGroup
	workerWG.Add(3)

	// 1. Publish pending outbox events to Redis.
	go func() {
		defer workerWG.Done()
		runOutboxPublisher(workerCtx, outboxPublisher)
	}()

	// 2. Consume Redis events and enqueue matching webhook deliveries.
	workerErr := make(chan error, 1)
	go func() {
		defer workerWG.Done()

		if err := runner.Run(workerCtx); err != nil {
			select {
			case workerErr <- fmt.Errorf("event worker: %w", err):
			default:
			}
		}
	}()

	// 3. Deliver webhooks and schedule retries for failed attempts.
	go func() {
		defer workerWG.Done()

		log.Printf("webhook delivery worker started: %s", deliveryWorkerID)
		deliveryWorker.Run(workerCtx)
		log.Println("webhook delivery worker stopped")
	}()

	// Configure HTTP routes.
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handler.HealthHandler)
	mux.Handle(
		"/v1/events",
		handler.RequireBearerToken(
			os.Getenv("EVENT_INGESTION_API_TOKEN"),
			handler.CreateEventHandler(eventRepo),
		),
	)

	mux.Handle(
		"/v1/subscriptions",
		handler.RequireBearerToken(
			os.Getenv("SUBSCRIPTION_API_TOKEN"),
			handler.CreateSubscriptionHandler(subscriptionService),
		),
	)

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

	// Wait for a shutdown signal or a fatal server/consumer error.
	stopCtx, stopSignal := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignal()

	select {
	case <-stopCtx.Done():
		log.Println("shutdown signal received")

	case err := <-serverErr:
		log.Printf("HTTP server failed: %v", err)

	case err := <-workerErr:
		log.Printf("event worker failed: %v", err)
	}

	// Stop background workers and gracefully shut down HTTP.
	stopWorkers()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful HTTP shutdown failed: %v", err)
	}

	// Wait for all three background workers to exit.
	workersDone := make(chan struct{})
	go func() {
		workerWG.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		log.Println("background workers stopped")

	case <-shutdownCtx.Done():
		log.Println("timed out waiting for background workers")
	}
}

func runOutboxPublisher(
	ctx context.Context,
	outboxPublisher *publisher.OutboxPublisher,
) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			log.Println("outbox publisher stopped")
			return
		}

		processed, err := outboxPublisher.PublishOne(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("outbox publisher stopped")
				return
			}

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
