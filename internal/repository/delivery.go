package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalidDelivery = errors.New("invalid webhook delivery")

type WebhookDelivery struct {
	ID             int64
	EventID        string
	SubscriptionID string
	Status         string
	Attempts       int
	NextAttemptAt  time.Time
	LastStatusCode sql.NullInt64
	LastError      sql.NullString
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeliveredAt    sql.NullTime
	LockedBy       sql.NullString
	LockedUntil    sql.NullTime
}

type DeliveryRepository struct {
	db *sql.DB
}

func NewDeliveryRepository(db *sql.DB) *DeliveryRepository {
	return &DeliveryRepository{db: db}
}

// Enqueue creates one delivery per event/subscription pair.
// Repeated calls are safe because the database enforces uniqueness.
func (r *DeliveryRepository) Enqueue(
	ctx context.Context,
	eventID string,
	subscriptionID string,
) error {
	if r == nil || r.db == nil {
		return errors.New("delivery database is not configured")
	}

	eventID = strings.TrimSpace(eventID)
	subscriptionID = strings.TrimSpace(subscriptionID)

	if eventID == "" || subscriptionID == "" {
		return ErrInvalidDelivery
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO webhook_deliveries (
			event_id,
			subscription_id
		)
		VALUES ($1, $2)
		ON CONFLICT (event_id, subscription_id) DO NOTHING
	`, eventID, subscriptionID)
	if err != nil {
		return fmt.Errorf("enqueue webhook delivery: %w", err)
	}

	return nil
}

// ListReady returns pending deliveries whose retry time has arrived
// and whose lease is absent or expired.
//
// This is a read-only query. Use ClaimReady when workers process
// deliveries concurrently.
func (r *DeliveryRepository) ListReady(
	ctx context.Context,
	limit int,
) ([]WebhookDelivery, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("delivery database is not configured")
	}

	if limit < 1 || limit > 500 {
		return nil, fmt.Errorf(
			"%w: limit must be between 1 and 500",
			ErrInvalidDelivery,
		)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id,
			event_id,
			subscription_id,
			status,
			attempts,
			next_attempt_at,
			last_status_code,
			last_error,
			created_at,
			updated_at,
			delivered_at,
			locked_by,
			locked_until
		FROM webhook_deliveries
		WHERE status = 'pending'
		  AND next_attempt_at <= NOW()
		  AND (
			locked_until IS NULL OR
			locked_until <= NOW()
		  )
		ORDER BY next_attempt_at, id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list ready webhook deliveries: %w", err)
	}
	defer rows.Close()

	deliveries := make([]WebhookDelivery, 0)

	for rows.Next() {
		var d WebhookDelivery
		if err := rows.Scan(
			&d.ID,
			&d.EventID,
			&d.SubscriptionID,
			&d.Status,
			&d.Attempts,
			&d.NextAttemptAt,
			&d.LastStatusCode,
			&d.LastError,
			&d.CreatedAt,
			&d.UpdatedAt,
			&d.DeliveredAt,
			&d.LockedBy,
			&d.LockedUntil,
		); err != nil {
			return nil, fmt.Errorf("scan webhook delivery: %w", err)
		}

		deliveries = append(deliveries, d)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhook deliveries: %w", err)
	}

	return deliveries, nil
}

// ClaimReady atomically claims eligible deliveries for one worker.
// A lease expires if the worker crashes or stops processing.
func (r *DeliveryRepository) ClaimReady(
	ctx context.Context,
	workerID string,
	limit int,
	leaseDuration time.Duration,
) ([]WebhookDelivery, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("delivery database is not configured")
	}

	workerID = strings.TrimSpace(workerID)

	if workerID == "" ||
		len(workerID) > 200 ||
		limit < 1 ||
		limit > 500 ||
		leaseDuration <= 0 ||
		leaseDuration > 15*time.Minute {
		return nil, ErrInvalidDelivery
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin delivery claim: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.QueryContext(ctx, `
		WITH candidates AS (
			SELECT id
			FROM webhook_deliveries
			WHERE status = 'pending'
			  AND next_attempt_at <= NOW()
			  AND (
				locked_until IS NULL OR
				locked_until <= NOW()
			  )
			ORDER BY next_attempt_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE webhook_deliveries AS d
		SET locked_by = $2,
		    locked_until = NOW() + ($3 * INTERVAL '1 second'),
		    updated_at = NOW()
		FROM candidates AS c
		WHERE d.id = c.id
		RETURNING
			d.id,
			d.event_id,
			d.subscription_id,
			d.status,
			d.attempts,
			d.next_attempt_at,
			d.last_status_code,
			d.last_error,
			d.created_at,
			d.updated_at,
			d.delivered_at,
			d.locked_by,
			d.locked_until
	`, limit, workerID, leaseDuration.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim ready webhook deliveries: %w", err)
	}

	deliveries := make([]WebhookDelivery, 0)

	for rows.Next() {
		var d WebhookDelivery
		if err := rows.Scan(
			&d.ID,
			&d.EventID,
			&d.SubscriptionID,
			&d.Status,
			&d.Attempts,
			&d.NextAttemptAt,
			&d.LastStatusCode,
			&d.LastError,
			&d.CreatedAt,
			&d.UpdatedAt,
			&d.DeliveredAt,
			&d.LockedBy,
			&d.LockedUntil,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan claimed webhook delivery: %w", err)
		}

		deliveries = append(deliveries, d)
	}

	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate claimed webhook deliveries: %w", err)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close claimed delivery rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit delivery claim: %w", err)
	}

	return deliveries, nil
}

// MarkDelivered records a successful HTTP delivery.
// Note: lease ownership is not checked by this method yet.
func (r *DeliveryRepository) MarkDelivered(
	ctx context.Context,
	id int64,
	statusCode int,
) error {
	if r == nil || r.db == nil {
		return errors.New("delivery database is not configured")
	}

	if id < 1 || statusCode < 200 || statusCode >= 300 {
		return ErrInvalidDelivery
	}

	result, err := r.db.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET status = 'delivered',
		    last_status_code = $2,
		    last_error = NULL,
		    delivered_at = NOW(),
		    updated_at = NOW(),
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1
		  AND status = 'pending'
	`, id, statusCode)
	if err != nil {
		return fmt.Errorf("mark webhook delivery delivered: %w", err)
	}

	return requireOneDeliveryRow(result)
}

// ScheduleRetry records a failed attempt and its next retry time.
// Note: lease ownership is not checked by this method yet.
func (r *DeliveryRepository) ScheduleRetry(
	ctx context.Context,
	id int64,
	statusCode int,
	lastError string,
	nextAttemptAt time.Time,
) error {
	if r == nil || r.db == nil {
		return errors.New("delivery database is not configured")
	}

	if id < 1 ||
		statusCode < 0 ||
		statusCode > 599 ||
		strings.TrimSpace(lastError) == "" ||
		nextAttemptAt.IsZero() {
		return ErrInvalidDelivery
	}

	// Bound stored error details to avoid unbounded database growth.
	if len(lastError) > 1000 {
		lastError = lastError[:1000]
	}

	result, err := r.db.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET attempts = attempts + 1,
		    last_status_code = NULLIF($2, 0),
		    last_error = $3,
		    next_attempt_at = $4,
		    updated_at = NOW(),
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1
		  AND status = 'pending'
	`, id, statusCode, lastError, nextAttemptAt)
	if err != nil {
		return fmt.Errorf("schedule webhook delivery retry: %w", err)
	}

	return requireOneDeliveryRow(result)
}

// MarkDeliveredByWorker records success only while the worker owns
// an unexpired lease on a pending delivery.
func (r *DeliveryRepository) MarkDeliveredByWorker(
	ctx context.Context,
	id int64,
	workerID string,
	statusCode int,
) error {
	if r == nil || r.db == nil {
		return errors.New("delivery database is not configured")
	}

	workerID = strings.TrimSpace(workerID)
	if id < 1 || workerID == "" || len(workerID) > 200 ||
		statusCode < 200 || statusCode >= 300 {
		return ErrInvalidDelivery
	}

	result, err := r.db.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET status = 'delivered',
		    last_status_code = $3,
		    last_error = NULL,
		    delivered_at = NOW(),
		    updated_at = NOW(),
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1
		  AND status = 'pending'
		  AND locked_by = $2
		  AND locked_until > NOW()
	`, id, workerID, statusCode)
	if err != nil {
		return fmt.Errorf("mark webhook delivery delivered: %w", err)
	}

	return requireOneDeliveryRow(result)
}

// ScheduleRetryByWorker schedules a retry only while the worker
// owns an unexpired lease on a pending delivery.
func (r *DeliveryRepository) ScheduleRetryByWorker(
	ctx context.Context,
	id int64,
	workerID string,
	statusCode int,
	lastError string,
	nextAttemptAt time.Time,
) error {
	if r == nil || r.db == nil {
		return errors.New("delivery database is not configured")
	}

	workerID = strings.TrimSpace(workerID)
	if id < 1 || workerID == "" || len(workerID) > 200 ||
		statusCode < 0 || statusCode > 599 ||
		strings.TrimSpace(lastError) == "" ||
		nextAttemptAt.IsZero() {
		return ErrInvalidDelivery
	}

	if len(lastError) > 1000 {
		lastError = lastError[:1000]
	}

	result, err := r.db.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET attempts = attempts + 1,
		    last_status_code = NULLIF($3, 0),
		    last_error = $4,
		    next_attempt_at = $5,
		    updated_at = NOW(),
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1
		  AND status = 'pending'
		  AND locked_by = $2
		  AND locked_until > NOW()
	`, id, workerID, statusCode, lastError, nextAttemptAt)
	if err != nil {
		return fmt.Errorf("schedule webhook delivery retry: %w", err)
	}

	return requireOneDeliveryRow(result)
}

// MarkDeadByWorker permanently fails a delivery only while the worker
// owns an unexpired lease on a pending delivery.
func (r *DeliveryRepository) MarkDeadByWorker(
	ctx context.Context,
	id int64,
	workerID string,
	statusCode int,
	lastError string,
) error {
	if r == nil || r.db == nil {
		return errors.New("delivery database is not configured")
	}

	workerID = strings.TrimSpace(workerID)
	lastError = strings.TrimSpace(lastError)

	if id < 1 ||
		workerID == "" ||
		len(workerID) > 200 ||
		statusCode < 0 ||
		statusCode > 599 ||
		lastError == "" {
		return ErrInvalidDelivery
	}

	if len(lastError) > 1000 {
		lastError = lastError[:1000]
	}

	result, err := r.db.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET status = 'dead',
		    attempts = attempts + 1,
		    last_status_code = NULLIF($3, 0),
		    last_error = $4,
		    updated_at = NOW(),
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1
		  AND status = 'pending'
		  AND locked_by = $2
		  AND locked_until > NOW()
	`, id, workerID, statusCode, lastError)
	if err != nil {
		return fmt.Errorf("mark webhook delivery dead: %w", err)
	}

	return requireOneDeliveryRow(result)
}

func requireOneDeliveryRow(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check webhook delivery update: %w", err)
	}

	if affected != 1 {
		return sql.ErrNoRows
	}

	return nil
}
