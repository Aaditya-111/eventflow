package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const DeadLetterStream = "eventflow:dead-letter"

type DeadLetterSink interface {
	Publish(ctx context.Context, message StreamMessage, attempts int, reason string) error
}

type RedisDeadLetterSink struct {
	client *redis.Client
	stream string
}

func NewRedisDeadLetterSink(client *redis.Client) *RedisDeadLetterSink {
	return &RedisDeadLetterSink{
		client: client,
		stream: DeadLetterStream,
	}
}

func (s *RedisDeadLetterSink) Publish(
	ctx context.Context,
	message StreamMessage,
	attempts int,
	reason string,
) error {
	if s == nil || s.client == nil {
		return errors.New("dead-letter Redis client is not configured")
	}
	if message.ID == "" {
		return errors.New("dead-letter source message ID is empty")
	}
	if attempts < 0 {
		return errors.New("attempt count cannot be negative")
	}

	values := make(map[string]interface{}, len(message.Values)+4)
	for key, value := range message.Values {
		values[key] = value
	}
	values["source_message_id"] = message.ID
	values["attempts"] = attempts
	values["failure_reason"] = reason
	values["dead_lettered_at"] = time.Now().UTC().Format(time.RFC3339Nano)

	if err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.stream,
		Values: values,
	}).Err(); err != nil {
		return fmt.Errorf("publish dead-letter message: %w", err)
	}
	return nil
}
