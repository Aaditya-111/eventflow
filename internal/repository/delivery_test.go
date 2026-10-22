package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func openTestDeliveryRepository(t *testing.T) *DeliveryRepository {
	t.Helper()

	dsn := os.Getenv("EVENTFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set EVENTFLOW_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("connect to test database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return NewDeliveryRepository(db)
}

func TestClaimReadyAndMarkDeliveredByOwner(t *testing.T) {
	repo := openTestDeliveryRepository(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	eventID := "delivery-test-" + suffix
	subscriptionID := "delivery-sub-test-" + suffix

	// These tests use existing foreign-key relationships.
	// Remove only rows created by this test.
	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM webhook_deliveries WHERE event_id = $1", eventID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM webhook_subscriptions WHERE subscription_id = $1", subscriptionID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM outbox WHERE event_id = $1", eventID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM events WHERE event_id = $1", eventID)
	})

	// Create the parent event and subscription using the schema's
	// minimum required fields.
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_type, data)
		VALUES ($1, 'delivery.test', '{}'::jsonb)
	`, eventID)
	if err != nil {
		t.Fatalf("create test event: %v", err)
	}

	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO webhook_subscriptions (
			subscription_id,
			endpoint_url,
			secret_encrypted,
			event_types,
			active
		)
		VALUES ($1, 'https://example.com/webhook', $2, ARRAY['delivery.test'], TRUE)
	`, subscriptionID, []byte("test-encrypted-secret"))
	if err != nil {
		t.Fatalf("create test subscription: %v", err)
	}

	if err := repo.Enqueue(ctx, eventID, subscriptionID); err != nil {
		t.Fatalf("enqueue delivery: %v", err)
	}

	deliveries, err := repo.ClaimReady(ctx, "worker-a", 10, time.Minute)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 claimed delivery, got %d", len(deliveries))
	}

	delivery := deliveries[0]
	if !delivery.LockedBy.Valid || delivery.LockedBy.String != "worker-a" {
		t.Fatalf("expected lease owner worker-a, got %+v", delivery.LockedBy)
	}

	err = repo.MarkDeliveredByWorker(ctx, delivery.ID, "worker-a", 204)
	if err != nil {
		t.Fatalf("owner should mark delivery delivered: %v", err)
	}

	var status string
	if err := repo.db.QueryRowContext(ctx,
		"SELECT status FROM webhook_deliveries WHERE id = $1",
		delivery.ID,
	).Scan(&status); err != nil {
		t.Fatalf("read delivery status: %v", err)
	}
	if status != "delivered" {
		t.Fatalf("expected delivered status, got %q", status)
	}
}

func TestMarkDeliveredByWorkerRejectsWrongOwner(t *testing.T) {
	repo := openTestDeliveryRepository(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	eventID := "delivery-owner-test-" + suffix
	subscriptionID := "delivery-owner-sub-" + suffix

	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM webhook_deliveries WHERE event_id = $1", eventID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM webhook_subscriptions WHERE subscription_id = $1", subscriptionID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM events WHERE event_id = $1", eventID)
	})

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_type, data)
		VALUES ($1, 'delivery.test', '{}'::jsonb)
	`, eventID)
	if err != nil {
		t.Fatalf("create test event: %v", err)
	}

	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO webhook_subscriptions (
			subscription_id,
			endpoint_url,
			secret_encrypted,
			event_types,
			active
		)
		VALUES ($1, 'https://example.com/webhook', $2, ARRAY['delivery.test'], TRUE)
	`, subscriptionID, []byte("test-encrypted-secret"))
	if err != nil {
		t.Fatalf("create test subscription: %v", err)
	}

	if err := repo.Enqueue(ctx, eventID, subscriptionID); err != nil {
		t.Fatalf("enqueue delivery: %v", err)
	}

	deliveries, err := repo.ClaimReady(ctx, "worker-a", 10, time.Minute)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 claimed delivery, got %d", len(deliveries))
	}

	err = repo.MarkDeliveredByWorker(ctx, deliveries[0].ID, "worker-b", 204)
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows for wrong owner, got %v", err)
	}
}

func TestMarkDeliveredByWorkerRejectsExpiredLease(t *testing.T) {
	repo := openTestDeliveryRepository(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	eventID := "delivery-expired-test-" + suffix
	subscriptionID := "delivery-expired-sub-" + suffix

	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM webhook_deliveries WHERE event_id = $1", eventID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM webhook_subscriptions WHERE subscription_id = $1", subscriptionID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM events WHERE event_id = $1", eventID)
	})

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_type, data)
		VALUES ($1, 'delivery.test', '{}'::jsonb)
	`, eventID)
	if err != nil {
		t.Fatalf("create test event: %v", err)
	}

	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO webhook_subscriptions (
			subscription_id,
			endpoint_url,
			secret_encrypted,
			event_types,
			active
		)
		VALUES ($1, 'https://example.com/webhook', $2, ARRAY['delivery.test'], TRUE)
	`, subscriptionID, []byte("test-encrypted-secret"))
	if err != nil {
		t.Fatalf("create test subscription: %v", err)
	}

	if err := repo.Enqueue(ctx, eventID, subscriptionID); err != nil {
		t.Fatalf("enqueue delivery: %v", err)
	}

	deliveries, err := repo.ClaimReady(ctx, "worker-a", 10, time.Minute)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 claimed delivery, got %d", len(deliveries))
	}

	_, err = repo.db.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET locked_until = NOW() - INTERVAL '1 second'
		WHERE id = $1
	`, deliveries[0].ID)
	if err != nil {
		t.Fatalf("expire test lease: %v", err)
	}

	err = repo.MarkDeliveredByWorker(ctx, deliveries[0].ID, "worker-a", 204)
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows for expired lease, got %v", err)
	}
}

func TestScheduleRetryByWorker(t *testing.T) {
	repo := openTestDeliveryRepository(t)
	ctx := context.Background()

	eventID := fmt.Sprintf("test-retry-%d", time.Now().UnixNano())
	subscriptionID := fmt.Sprintf("test-sub-retry-%d", time.Now().UnixNano())

	_, err := repo.db.ExecContext(ctx,
		`INSERT INTO events (event_id, event_type, data)
		 VALUES ($1, $2, $3::jsonb)`,
		eventID, "test.retry", `{"test":true}`,
	)
	if err != nil {
		t.Fatalf("create test event: %v", err)
	}

	_, err = repo.db.ExecContext(ctx,
		`INSERT INTO webhook_subscriptions
		 (subscription_id, endpoint_url, secret_encrypted, event_types, active)
		 VALUES ($1, $2, $3, $4, $5)`,
		subscriptionID, "https://example.com/webhook",
		[]byte("test-secret"), []string{"test.retry"}, true,
	)
	if err != nil {
		t.Fatalf("create test subscription: %v", err)
	}

	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(ctx,
			`DELETE FROM webhook_deliveries
			 WHERE event_id = $1 AND subscription_id = $2`,
			eventID, subscriptionID,
		)
		_, _ = repo.db.ExecContext(ctx,
			`DELETE FROM webhook_subscriptions WHERE subscription_id = $1`,
			subscriptionID,
		)
		_, _ = repo.db.ExecContext(ctx,
			`DELETE FROM outbox WHERE event_id = $1`, eventID,
		)
		_, _ = repo.db.ExecContext(ctx,
			`DELETE FROM events WHERE event_id = $1`, eventID,
		)
	})

	if err := repo.Enqueue(ctx, eventID, subscriptionID); err != nil {
		t.Fatalf("enqueue delivery: %v", err)
	}

	claimed, err := repo.ClaimReady(ctx, "retry-worker", 10, time.Minute)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}

	var delivery *WebhookDelivery
	for i := range claimed {
		if claimed[i].EventID == eventID &&
			claimed[i].SubscriptionID == subscriptionID {
			delivery = &claimed[i]
			break
		}
	}
	if delivery == nil {
		t.Fatal("expected delivery to be claimed")
	}

	nextAttempt := time.Now().Add(30 * time.Second)
	err = repo.ScheduleRetryByWorker(
		ctx, delivery.ID, "retry-worker", 503,
		"temporary upstream failure", nextAttempt,
	)
	if err != nil {
		t.Fatalf("schedule retry: %v", err)
	}

	var status string
	var attempts int
	var lastStatus int
	var lastError string
	var lockedBy sql.NullString

	err = repo.db.QueryRowContext(ctx,
		`SELECT status, attempts, last_status_code, last_error, locked_by
		 FROM webhook_deliveries WHERE id = $1`,
		delivery.ID,
	).Scan(&status, &attempts, &lastStatus, &lastError, &lockedBy)
	if err != nil {
		t.Fatalf("read delivery: %v", err)
	}

	if status != "pending" || attempts != 1 || lastStatus != 503 ||
		lastError != "temporary upstream failure" || lockedBy.Valid {
		t.Fatalf("unexpected retry state: status=%s attempts=%d code=%d error=%q locked=%v",
			status, attempts, lastStatus, lastError, lockedBy)
	}
}

func TestScheduleRetryByWorkerRejectsWrongOwner(t *testing.T) {
	repo := openTestDeliveryRepository(t)
	ctx := context.Background()

	eventID := fmt.Sprintf("test-retry-owner-%d", time.Now().UnixNano())
	subscriptionID := fmt.Sprintf("test-sub-owner-%d", time.Now().UnixNano())

	_, err := repo.db.ExecContext(ctx,
		`INSERT INTO events (event_id, event_type, data)
		 VALUES ($1, $2, $3::jsonb)`,
		eventID, "test.retry", `{"test":true}`,
	)
	if err != nil {
		t.Fatalf("create test event: %v", err)
	}

	_, err = repo.db.ExecContext(ctx,
		`INSERT INTO webhook_subscriptions
		 (subscription_id, endpoint_url, secret_encrypted, event_types, active)
		 VALUES ($1, $2, $3, $4, $5)`,
		subscriptionID, "https://example.com/webhook",
		[]byte("test-secret"), []string{"test.retry"}, true,
	)
	if err != nil {
		t.Fatalf("create test subscription: %v", err)
	}

	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(ctx,
			`DELETE FROM webhook_deliveries
			 WHERE event_id = $1 AND subscription_id = $2`,
			eventID, subscriptionID,
		)
		_, _ = repo.db.ExecContext(ctx,
			`DELETE FROM webhook_subscriptions WHERE subscription_id = $1`,
			subscriptionID,
		)
		_, _ = repo.db.ExecContext(ctx,
			`DELETE FROM outbox WHERE event_id = $1`, eventID,
		)
		_, _ = repo.db.ExecContext(ctx,
			`DELETE FROM events WHERE event_id = $1`, eventID,
		)
	})

	if err := repo.Enqueue(ctx, eventID, subscriptionID); err != nil {
		t.Fatalf("enqueue delivery: %v", err)
	}

	claimed, err := repo.ClaimReady(ctx, "retry-worker-a", 10, time.Minute)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}

	var delivery *WebhookDelivery
	for i := range claimed {
		if claimed[i].EventID == eventID &&
			claimed[i].SubscriptionID == subscriptionID {
			delivery = &claimed[i]
			break
		}
	}
	if delivery == nil {
		t.Fatal("expected delivery to be claimed")
	}

	err = repo.ScheduleRetryByWorker(
		ctx, delivery.ID, "retry-worker-b", 503,
		"temporary upstream failure", time.Now().Add(time.Minute),
	)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for wrong owner, got %v", err)
	}
}

func TestClaimReadyConcurrentWorkersDoNotClaimSameDelivery(t *testing.T) {
	repo := openTestDeliveryRepository(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	const deliveryCount = 4

	eventIDs := make([]string, deliveryCount)
	subscriptionIDs := make([]string, deliveryCount)

	t.Cleanup(func() {
		for i := 0; i < deliveryCount; i++ {
			_, _ = repo.db.ExecContext(ctx,
				"DELETE FROM webhook_deliveries WHERE event_id = $1", eventIDs[i])
			_, _ = repo.db.ExecContext(ctx,
				"DELETE FROM webhook_subscriptions WHERE subscription_id = $1",
				subscriptionIDs[i])
			_, _ = repo.db.ExecContext(ctx,
				"DELETE FROM outbox WHERE event_id = $1", eventIDs[i])
			_, _ = repo.db.ExecContext(ctx,
				"DELETE FROM events WHERE event_id = $1", eventIDs[i])
		}
	})

	for i := 0; i < deliveryCount; i++ {
		eventIDs[i] = fmt.Sprintf("delivery-concurrent-%s-%d", suffix, i)
		subscriptionIDs[i] = fmt.Sprintf("delivery-concurrent-sub-%s-%d", suffix, i)

		_, err := repo.db.ExecContext(ctx, `
			INSERT INTO events (event_id, event_type, data)
			VALUES ($1, 'delivery.test', '{}'::jsonb)
		`, eventIDs[i])
		if err != nil {
			t.Fatalf("create event %d: %v", i, err)
		}

		_, err = repo.db.ExecContext(ctx, `
			INSERT INTO webhook_subscriptions (
				subscription_id,
				endpoint_url,
				secret_encrypted,
				event_types,
				active
			)
			VALUES ($1, 'https://example.com/webhook',
			        $2, ARRAY['delivery.test'], TRUE)
		`, subscriptionIDs[i], []byte("test-encrypted-secret"))
		if err != nil {
			t.Fatalf("create subscription %d: %v", i, err)
		}

		if err := repo.Enqueue(ctx, eventIDs[i], subscriptionIDs[i]); err != nil {
			t.Fatalf("enqueue delivery %d: %v", i, err)
		}
	}

	type claimResult struct {
		workerID   string
		deliveries []WebhookDelivery
		err        error
	}

	start := make(chan struct{})
	results := make(chan claimResult, 2)

	for _, workerID := range []string{"concurrent-worker-a", "concurrent-worker-b"} {
		go func(id string) {
			<-start
			deliveries, err := repo.ClaimReady(ctx, id, deliveryCount, time.Minute)
			results <- claimResult{
				workerID:   id,
				deliveries: deliveries,
				err:        err,
			}
		}(workerID)
	}

	// Release both workers as close together as possible.
	close(start)

	first := <-results
	second := <-results

	if first.err != nil {
		t.Fatalf("%s claim failed: %v", first.workerID, first.err)
	}
	if second.err != nil {
		t.Fatalf("%s claim failed: %v", second.workerID, second.err)
	}

	seen := make(map[int64]string)

	for _, result := range []claimResult{first, second} {
		for _, delivery := range result.deliveries {
			if previousWorker, exists := seen[delivery.ID]; exists {
				t.Errorf(
					"delivery %d claimed by both %s and %s",
					delivery.ID, previousWorker, result.workerID,
				)
				continue
			}

			seen[delivery.ID] = result.workerID

			if !delivery.LockedBy.Valid ||
				delivery.LockedBy.String != result.workerID {
				t.Errorf(
					"delivery %d owner = %+v; want %s",
					delivery.ID, delivery.LockedBy, result.workerID,
				)
			}
		}
	}

	if len(seen) != deliveryCount {
		t.Errorf("workers claimed %d unique deliveries; want %d",
			len(seen), deliveryCount)
	}
}

func TestMarkDeadByWorker(t *testing.T) {
	repo := openTestDeliveryRepository(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	eventID := "delivery-dead-test-" + suffix
	subscriptionID := "delivery-dead-sub-" + suffix

	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM webhook_deliveries WHERE event_id = $1", eventID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM webhook_subscriptions WHERE subscription_id = $1", subscriptionID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM outbox WHERE event_id = $1", eventID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM events WHERE event_id = $1", eventID)
	})

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_type, data)
		VALUES ($1, 'delivery.test', '{}'::jsonb)
	`, eventID)
	if err != nil {
		t.Fatalf("create test event: %v", err)
	}

	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO webhook_subscriptions (
			subscription_id,
			endpoint_url,
			secret_encrypted,
			event_types,
			active
		)
		VALUES ($1, 'https://example.com/webhook',
		        $2, ARRAY['delivery.test'], TRUE)
	`, subscriptionID, []byte("test-encrypted-secret"))
	if err != nil {
		t.Fatalf("create test subscription: %v", err)
	}

	if err := repo.Enqueue(ctx, eventID, subscriptionID); err != nil {
		t.Fatalf("enqueue delivery: %v", err)
	}

	claimed, err := repo.ClaimReady(ctx, "dead-worker", 10, time.Minute)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d deliveries; want 1", len(claimed))
	}

	deliveryID := claimed[0].ID
	const failure = "permanent upstream failure"

	if err := repo.MarkDeadByWorker(
		ctx, deliveryID, "dead-worker", 410, failure,
	); err != nil {
		t.Fatalf("mark delivery dead: %v", err)
	}

	var status string
	var attempts int
	var lastStatus sql.NullInt64
	var lastError sql.NullString
	var lockedBy sql.NullString
	var lockedUntil sql.NullTime

	err = repo.db.QueryRowContext(ctx, `
		SELECT status, attempts, last_status_code, last_error,
		       locked_by, locked_until
		FROM webhook_deliveries
		WHERE id = $1
	`, deliveryID).Scan(
		&status, &attempts, &lastStatus, &lastError,
		&lockedBy, &lockedUntil,
	)
	if err != nil {
		t.Fatalf("read dead delivery: %v", err)
	}

	if status != "dead" {
		t.Errorf("status = %q; want dead", status)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d; want 1", attempts)
	}
	if !lastStatus.Valid || lastStatus.Int64 != 410 {
		t.Errorf("last status = %+v; want 410", lastStatus)
	}
	if !lastError.Valid || lastError.String != failure {
		t.Errorf("last error = %+v; want %q", lastError, failure)
	}
	if lockedBy.Valid || lockedUntil.Valid {
		t.Errorf("lease should be cleared; locked_by=%+v locked_until=%+v",
			lockedBy, lockedUntil)
	}

	// Dead deliveries must not be claimed again.
	reclaimed, err := repo.ClaimReady(ctx, "another-worker", 10, time.Minute)
	if err != nil {
		t.Fatalf("claim after dead-lettering: %v", err)
	}
	for _, d := range reclaimed {
		if d.ID == deliveryID {
			t.Fatalf("dead delivery %d was claimed again", deliveryID)
		}
	}
}
