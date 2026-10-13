package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type Runner struct {
	consumer    *Consumer
	processor   *Processor
	handler     EventHandler
	deadLetters DeadLetterSink
	policy      RetryPolicy
	batchSize   int64
	minIdle     time.Duration
}

func NewRunner(
	consumer *Consumer,
	processor *Processor,
	handler EventHandler,
	deadLetters DeadLetterSink,
	policy RetryPolicy,
) (*Runner, error) {
	if consumer == nil {
		return nil, errors.New("worker consumer is not configured")
	}
	if processor == nil {
		return nil, errors.New("worker processor is not configured")
	}
	if handler == nil {
		return nil, errors.New("worker event handler is not configured")
	}
	if deadLetters == nil {
		return nil, errors.New("worker dead-letter sink is not configured")
	}
	if policy.MaxAttempts < 1 ||
		policy.InitialBackoff < 0 ||
		policy.MaxBackoff < policy.InitialBackoff {
		return nil, errors.New("invalid worker retry policy")
	}

	return &Runner{
		consumer:    consumer,
		processor:   processor,
		handler:     handler,
		deadLetters: deadLetters,
		policy:      policy,
		batchSize:   10,
		minIdle:     30 * time.Second,
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("worker runner is nil")
	}

	if err := r.consumer.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("ensure worker consumer group: %w", err)
	}

	log.Println("EventFlow worker started")

	reclaimCursor := "0-0"

	for {
		if err := ctx.Err(); err != nil {
			log.Println("EventFlow worker stopped")
			return nil
		}

		messages, nextCursor, err := r.consumer.ReclaimPending(
			ctx, reclaimCursor, r.batchSize, r.minIdle,
		)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("worker pending-message recovery: %v", err)
		} else {
			reclaimCursor = nextCursor
			for _, message := range messages {
				if err := ctx.Err(); err != nil {
					return nil
				}
				r.process(ctx, message)
			}

			// Start a fresh pending-message scan after reaching
			// the end of the current scan.
			if reclaimCursor == "0-0" {
				reclaimCursor = "0"
			}
		}

		messages, err = r.consumer.ReadNew(ctx, r.batchSize, 1000)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("worker read: %v", err)
			continue
		}

		for _, message := range messages {
			if err := ctx.Err(); err != nil {
				return nil
			}
			r.process(ctx, message)
		}
	}
}

func (r *Runner) process(ctx context.Context, message StreamMessage) {
	if err := r.processor.ProcessWithRetry(
		ctx,
		message,
		r.handler,
		r.policy,
		r.deadLetters,
	); err != nil {
		log.Printf(
			"worker message %s remains unresolved: %v",
			message.ID,
			err,
		)
	}
}
