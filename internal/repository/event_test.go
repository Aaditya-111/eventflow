package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func openTestRepository(t *testing.T) *EventRepository {
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
		db.Close()
		t.Fatalf("connect to test database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return NewEventRepository(db)
}

func TestCreateEventAndOutbox(t *testing.T) {
	repo := openTestRepository(t)
	ctx := context.Background()

	eventID := fmt.Sprintf("test-%d", time.Now().UnixNano())
	event := Event{
		EventID: eventID,
		Type:    "user.created",
		Data:    json.RawMessage(`{"user_id":"test-user","plan":"pro"}`),
	}

	// Remove only this test's rows, even if an assertion fails.
	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(
			context.Background(),
			"DELETE FROM outbox WHERE event_id = $1",
			eventID,
		)
		_, _ = repo.db.ExecContext(
			context.Background(),
			"DELETE FROM events WHERE event_id = $1",
			eventID,
		)
	})

	created, err := repo.Create(ctx, event)
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	if !created {
		t.Fatal("expected a newly created event")
	}

	var eventCount, outboxCount int
	err = repo.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM events WHERE event_id = $1",
		eventID,
	).Scan(&eventCount)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}

	err = repo.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM outbox WHERE event_id = $1",
		eventID,
	).Scan(&outboxCount)
	if err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}

	if eventCount != 1 || outboxCount != 1 {
		t.Fatalf(
			"expected one event and one outbox row; got events=%d outbox=%d",
			eventCount, outboxCount,
		)
	}
}

func TestCreateIdenticalEventIsIdempotent(t *testing.T) {
	repo := openTestRepository(t)
	ctx := context.Background()

	eventID := fmt.Sprintf("test-idempotent-%d", time.Now().UnixNano())
	event := Event{
		EventID: eventID,
		Type:    "user.created",
		Data:    json.RawMessage(`{"user_id":"u1","plan":"pro"}`),
	}

	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM outbox WHERE event_id = $1", eventID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM events WHERE event_id = $1", eventID)
	})

	created, err := repo.Create(ctx, event)
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}

	// Object key order and whitespace should not change event identity.
	event.Data = json.RawMessage(`{ "plan" : "pro", "user_id" : "u1" }`)
	created, err = repo.Create(ctx, event)
	if err != nil {
		t.Fatalf("identical retry returned an error: %v", err)
	}
	if created {
		t.Fatal("identical retry must not create a second event")
	}

	var count int
	err = repo.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM outbox WHERE event_id = $1",
		eventID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one outbox row after retry; got %d", count)
	}
}

func TestCreateConflictingEventReturnsError(t *testing.T) {
	repo := openTestRepository(t)
	ctx := context.Background()

	eventID := fmt.Sprintf("test-conflict-%d", time.Now().UnixNano())
	event := Event{
		EventID: eventID,
		Type:    "user.created",
		Data:    json.RawMessage(`{"user_id":"u1"}`),
	}

	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM outbox WHERE event_id = $1", eventID)
		_, _ = repo.db.ExecContext(ctx,
			"DELETE FROM events WHERE event_id = $1", eventID)
	})

	if created, err := repo.Create(ctx, event); err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}

	event.Data = json.RawMessage(`{"user_id":"u2"}`)
	created, err := repo.Create(ctx, event)

	if created {
		t.Fatal("conflicting event must not be created")
	}
	if !errors.Is(err, ErrEventConflict) {
		t.Fatalf("expected ErrEventConflict, got %v", err)
	}

	var count int
	err = repo.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM outbox WHERE event_id = $1",
		eventID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one outbox row after conflict; got %d", count)
	}
}
