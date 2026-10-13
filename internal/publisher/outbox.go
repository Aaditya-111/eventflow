package publisher

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const EventStream = "eventflow:events"

type OutboxPublisher struct {
	db    *sql.DB
	redis *redis.Client
}

func NewOutboxPublisher(db *sql.DB, client *redis.Client) *OutboxPublisher {
	return &OutboxPublisher{
		db:    db,
		redis: client,
	}
}

// PublishOne publishes one pending outbox record.
// A record is marked published only after Redis acknowledges the write.
func (p *OutboxPublisher) PublishOne(ctx context.Context) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin outbox transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var (
		outboxID  int64
		eventID   string
		eventType string
		data      []byte
		createdAt time.Time
	)

	err = tx.QueryRowContext(ctx, `
		SELECT o.id, e.event_id, e.event_type, e.data, e.created_at
		FROM outbox AS o
		JOIN events AS e ON e.event_id = o.event_id
		WHERE o.published_at IS NULL
		  AND o.next_attempt_at <= NOW()
		ORDER BY o.id
		FOR UPDATE OF o SKIP LOCKED
		LIMIT 1
	`).Scan(&outboxID, &eventID, &eventType, &data, &createdAt)

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load pending outbox record: %w", err)
	}

	// Bound the Redis operation so a stalled Redis server cannot hold
	// the PostgreSQL row lock indefinitely.
	publishCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, publishErr := p.redis.XAdd(publishCtx, &redis.XAddArgs{
		Stream: EventStream,
		Values: map[string]interface{}{
			"event_id":   eventID,
			"type":       eventType,
			"data":       string(data),
			"created_at": createdAt.UTC().Format(time.RFC3339Nano),
		},
	}).Result()

	if publishErr != nil {
		message := publishErr.Error()
		if len(message) > 1000 {
			message = message[:1000]
		}

		_, updateErr := tx.ExecContext(ctx, `
			UPDATE outbox
			SET attempts = attempts + 1,
			    next_attempt_at = NOW() +
			        LEAST(300, POWER(2, LEAST(attempts, 8))) * INTERVAL '1 second',
			    last_error = $2
			WHERE id = $1
		`, outboxID, message)
		if updateErr != nil {
			return false, fmt.Errorf(
				"publish failed (%v); record retry failed: %w",
				publishErr, updateErr,
			)
		}

		if commitErr := tx.Commit(); commitErr != nil {
			return false, fmt.Errorf("commit failed-publish retry: %w", commitErr)
		}
		return true, fmt.Errorf("publish event %s to Redis: %w", eventID, publishErr)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE outbox
		SET published_at = NOW(),
		    attempts = attempts + 1,
		    last_error = NULL
		WHERE id = $1
	`, outboxID)
	if err != nil {
		return false, fmt.Errorf("mark outbox record published: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit published outbox record: %w", err)
	}

	return true, nil
}

// Keep this compile-time assertion close to the publisher implementation.
var _ = json.Valid
var _ = strings.TrimSpace
