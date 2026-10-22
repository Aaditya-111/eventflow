package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Aaditya-111/eventflow/internal/repository"
	"github.com/Aaditya-111/eventflow/internal/security"
)

const (
	deliveryBatchSize    = 5
	deliveryLease        = 60 * time.Second
	deliveryTimeout      = 10 * time.Second
	deliveryMaxTries     = 8
	deliveryPoll         = 500 * time.Millisecond
	deliveryMaxRetry     = 5 * time.Minute
	deliveryStateTimeout = 3 * time.Second
)

type DeliveryClaimer interface {
	ClaimReady(
		ctx context.Context,
		workerID string,
		limit int,
		leaseDuration time.Duration,
	) ([]repository.WebhookDelivery, error)
}

type DeliveryEventReader interface {
	GetByID(ctx context.Context, eventID string) (repository.Event, error)
}

type DeliveryTargetReader interface {
	GetActiveDeliveryTarget(
		ctx context.Context,
		subscriptionID string,
	) (repository.WebhookDeliveryTarget, error)
}

type DeliveryStateWriter interface {
	MarkDeliveredByWorker(
		ctx context.Context,
		id int64,
		workerID string,
		statusCode int,
	) error

	ScheduleRetryByWorker(
		ctx context.Context,
		id int64,
		workerID string,
		statusCode int,
		lastError string,
		nextAttemptAt time.Time,
	) error

	MarkDeadByWorker(
		ctx context.Context,
		id int64,
		workerID string,
		statusCode int,
		lastError string,
	) error
}

type DeliverySecretDecryptor interface {
	Decrypt(ciphertext []byte) ([]byte, error)
}

type WebhookDeliverySender interface {
	Send(
		ctx context.Context,
		target repository.WebhookDeliveryTarget,
		event repository.Event,
		secret []byte,
	) (WebhookResult, error)
}

type DeliveryWorker struct {
	id      string
	claimer DeliveryClaimer
	events  DeliveryEventReader
	targets DeliveryTargetReader
	states  DeliveryStateWriter
	secrets DeliverySecretDecryptor
	sender  WebhookDeliverySender
}

func NewDeliveryWorker(
	workerID string,
	claimer DeliveryClaimer,
	events DeliveryEventReader,
	targets DeliveryTargetReader,
	states DeliveryStateWriter,
	secrets DeliverySecretDecryptor,
	sender WebhookDeliverySender,
) (*DeliveryWorker, error) {
	workerID = strings.TrimSpace(workerID)

	if workerID == "" || len(workerID) > 200 {
		return nil, errors.New("valid delivery worker ID is required")
	}

	if claimer == nil || events == nil || targets == nil ||
		states == nil || secrets == nil || sender == nil {
		return nil, errors.New("delivery worker dependencies are required")
	}

	return &DeliveryWorker{
		id:      workerID,
		claimer: claimer,
		events:  events,
		targets: targets,
		states:  states,
		secrets: secrets,
		sender:  sender,
	}, nil
}

// Run polls PostgreSQL until the supplied context is cancelled.
// Temporary database errors are logged and retried on the next poll.
func (w *DeliveryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(deliveryPoll)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		deliveries, err := w.claimer.ClaimReady(
			ctx,
			w.id,
			deliveryBatchSize,
			deliveryLease,
		)

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			log.Printf("delivery worker: claim failed: %v", err)
		} else if len(deliveries) > 0 {
			var wg sync.WaitGroup

			for _, delivery := range deliveries {
				if ctx.Err() != nil {
					break
				}

				wg.Add(1)
				go func(d repository.WebhookDelivery) {
					defer wg.Done()
					w.process(ctx, d)
				}(delivery)
			}

			wg.Wait()
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *DeliveryWorker) process(
	parent context.Context,
	delivery repository.WebhookDelivery,
) {
	ctx, cancel := context.WithTimeout(parent, deliveryTimeout)
	defer cancel()

	// Retry-After is used only when supplied by the webhook sender.
	var retryAfter time.Duration

	fail := func(statusCode int, message string, permanent bool) {
		message = safeDeliveryError(message)

		stateCtx, cancelState := newDeliveryStateContext(parent)
		defer cancelState()

		var err error

		if permanent || delivery.Attempts+1 >= deliveryMaxTries {
			err = w.states.MarkDeadByWorker(
				stateCtx,
				delivery.ID,
				w.id,
				statusCode,
				message,
			)
		} else {
			delay := deliveryRetryDelay(delivery.Attempts + 1)

			if retryAfter > 0 {
				delay = retryAfter
				if delay > deliveryMaxRetry {
					delay = deliveryMaxRetry
				}
			}

			nextAttemptAt := time.Now().Add(delay)

			err = w.states.ScheduleRetryByWorker(
				stateCtx,
				delivery.ID,
				w.id,
				statusCode,
				message,
				nextAttemptAt,
			)
		}

		if err != nil {
			log.Printf(
				"delivery worker: unable to record outcome for delivery %d: %v",
				delivery.ID,
				err,
			)
		}
	}

	event, err := w.events.GetByID(ctx, delivery.EventID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			fail(0, "event not found", true)
		} else {
			fail(0, "event lookup failed", false)
		}
		return
	}

	target, err := w.targets.GetActiveDeliveryTarget(
		ctx,
		delivery.SubscriptionID,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			fail(0, "subscription is missing or inactive", true)
		} else {
			fail(0, "subscription lookup failed", false)
		}
		return
	}

	secret, err := w.secrets.Decrypt(target.EncryptedSecret)
	if err != nil {
		fail(0, "subscription secret decryption failed", true)
		return
	}
	defer clear(secret)

	result, sendErr := w.sender.Send(ctx, target, event, secret)

	if result.RetryAfter > 0 {
		retryAfter = result.RetryAfter
	}

	if sendErr == nil &&
		result.StatusCode >= 200 &&
		result.StatusCode < 300 {

		stateCtx, cancelState := newDeliveryStateContext(parent)
		defer cancelState()

		if err := w.states.MarkDeliveredByWorker(
			stateCtx,
			delivery.ID,
			w.id,
			result.StatusCode,
		); err != nil {
			log.Printf(
				"delivery worker: unable to mark delivery %d successful: %v",
				delivery.ID,
				err,
			)
		}

		return
	}
	statusCode := result.StatusCode
	if sendErr == nil {
		sendErr = fmt.Errorf("unexpected webhook response")
	}

	if isPermanentHTTPStatus(statusCode) {
		fail(statusCode, "webhook returned a permanent HTTP error", true)
		return
	}

	if errors.Is(sendErr, context.Canceled) ||
		errors.Is(sendErr, context.DeadlineExceeded) {
		fail(statusCode, "webhook request timed out or was cancelled", false)
		return
	}

	// Destination validation failures must not be retried indefinitely.
	// A typed security error would be preferable to string matching here.
	if errors.Is(sendErr, security.ErrUnsafeWebhookURL) {
		fail(statusCode, "webhook destination failed security validation", true)
		return
	}

	fail(statusCode, "webhook request failed", false)
}

func isPermanentHTTPStatus(statusCode int) bool {
	if statusCode < 400 || statusCode >= 500 {
		return false
	}

	switch statusCode {
	case 408, 425, 429:
		return false
	default:
		return true
	}
}

func deliveryRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := time.Second

	for i := 1; i < attempt && delay < deliveryMaxRetry; i++ {
		delay *= 2
	}

	if delay > deliveryMaxRetry {
		delay = deliveryMaxRetry
	}

	return delay
}

func safeDeliveryError(message string) string {
	message = strings.TrimSpace(message)

	if message == "" {
		return "webhook delivery failed"
	}

	// Avoid persisting common raw network address and port errors.
	if _, _, err := net.SplitHostPort(message); err == nil {
		return "webhook delivery failed"
	}

	if _, err := strconv.Atoi(message); err == nil {
		return "webhook delivery failed"
	}

	if len(message) > 200 {
		message = message[:200]
	}

	return message
}

// newDeliveryStateContext allows a bounded database state update
// to finish even when the webhook-processing context is cancelled.
func newDeliveryStateContext(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.WithoutCancel(parent),
		deliveryStateTimeout,
	)
}
