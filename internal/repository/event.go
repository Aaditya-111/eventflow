package repository

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrEventConflict = errors.New("event ID already exists with different content")
	ErrInvalidEvent  = errors.New("invalid event")
)

type Event struct {
	EventID   string          `json:"event_id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"created_at"`
}

type EventRepository struct {
	db *sql.DB
}

func NewEventRepository(db *sql.DB) *EventRepository {
	return &EventRepository{db: db}
}

// Create atomically stores a new event and its outbox record.
// It returns true only when a new event is created.
func (r *EventRepository) Create(
	ctx context.Context,
	event Event,
) (bool, error) {
	event.EventID = strings.TrimSpace(event.EventID)
	event.Type = strings.TrimSpace(event.Type)
	event.Data = json.RawMessage(bytes.TrimSpace(event.Data))

	if event.EventID == "" ||
		len(event.EventID) > 200 ||
		event.Type == "" ||
		len(event.Type) > 200 ||
		len(event.Data) == 0 ||
		!json.Valid(event.Data) {
		return false, ErrInvalidEvent
	}

	if event.Data[0] != '{' && event.Data[0] != '[' {
		return false, ErrInvalidEvent
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin event transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO events (event_id, event_type, data)
		 VALUES ($1, $2, $3::jsonb)
		 ON CONFLICT (event_id) DO NOTHING`,
		event.EventID,
		event.Type,
		string(event.Data),
	)
	if err != nil {
		return false, wrapDatabaseError("insert event", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check event insert result: %w", err)
	}

	if rows == 1 {
		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO outbox (event_id) VALUES ($1)`,
			event.EventID,
		)
		if err != nil {
			return false, wrapDatabaseError("insert outbox record", err)
		}

		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit event transaction: %w", err)
		}
		return true, nil
	}

	var existingType string
	var existingData []byte

	err = tx.QueryRowContext(
		ctx,
		`SELECT event_type, data
		 FROM events
		 WHERE event_id = $1
		 FOR UPDATE`,
		event.EventID,
	).Scan(&existingType, &existingData)
	if err != nil {
		return false, fmt.Errorf("read existing event: %w", err)
	}

	incomingJSON, err := canonicalJSON(event.Data)
	if err != nil {
		return false, ErrInvalidEvent
	}

	existingJSON, err := canonicalJSON(existingData)
	if err != nil {
		return false, fmt.Errorf("decode stored event: %w", err)
	}

	if existingType != event.Type ||
		!bytes.Equal(incomingJSON, existingJSON) {
		return false, ErrEventConflict
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit idempotency check: %w", err)
	}

	return false, nil
}

func canonicalJSON(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}

	return json.Marshal(value)
}

func wrapDatabaseError(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Errorf("%s: PostgreSQL error %s: %w",
			operation, pgErr.Code, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
