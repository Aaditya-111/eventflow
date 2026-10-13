package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	EventStream        = "eventflow:events"
	EventConsumerGroup = "eventflow:workers"
)

type StreamMessage struct {
	ID     string
	Values map[string]interface{}
}

type Consumer struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
}

func NewConsumer(client *redis.Client, group, consumer string) *Consumer {
	return newConsumerForStream(client, EventStream, group, consumer)
}

// Used by tests to isolate Redis stream operations from production events.
func newConsumerForStream(client *redis.Client, stream, group, consumer string) *Consumer {
	return &Consumer{
		client:   client,
		stream:   stream,
		group:    group,
		consumer: consumer,
	}
}

func (c *Consumer) EnsureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis consumer group: %w", err)
	}
	return nil
}

func (c *Consumer) ReadNew(
	ctx context.Context,
	count int64,
	blockMillis int64,
) ([]StreamMessage, error) {
	if count <= 0 {
		return nil, errors.New("count must be positive")
	}
	if blockMillis < 0 {
		return nil, errors.New("block duration cannot be negative")
	}

	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.consumer,
		Streams:  []string{c.stream, ">"},
		Count:    count,
		Block:    time.Duration(blockMillis) * time.Millisecond,
	}).Result()

	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Redis stream: %w", err)
	}

	var messages []StreamMessage
	for _, stream := range streams {
		for _, message := range stream.Messages {
			messages = append(messages, StreamMessage{
				ID:     message.ID,
				Values: message.Values,
			})
		}
	}
	return messages, nil
}

func (c *Consumer) Ack(ctx context.Context, messageID string) error {
	if strings.TrimSpace(messageID) == "" {
		return errors.New("message ID cannot be empty")
	}
	if err := c.client.XAck(ctx, c.stream, c.group, messageID).Err(); err != nil {
		return fmt.Errorf("acknowledge Redis message: %w", err)
	}
	return nil
}

// ReclaimPending transfers ownership of idle, unacknowledged messages to this
// consumer. The returned cursor can be supplied on the next call to continue
// scanning pending messages. Start with "0-0".
func (c *Consumer) ReclaimPending(
	ctx context.Context,
	start string,
	count int64,
	minIdle time.Duration,
) ([]StreamMessage, string, error) {
	if count <= 0 {
		return nil, start, errors.New("count must be positive")
	}
	if minIdle < 0 {
		return nil, start, errors.New("minimum idle duration cannot be negative")
	}
	if strings.TrimSpace(start) == "" {
		start = "0-0"
	}

	messages, next, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   c.stream,
		Group:    c.group,
		Consumer: c.consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if err != nil {
		return nil, start, fmt.Errorf("reclaim pending messages: %w", err)
	}

	result := make([]StreamMessage, 0, len(messages))
	for _, message := range messages {
		result = append(result, StreamMessage{
			ID:     message.ID,
			Values: message.Values,
		})
	}
	return result, next, nil
}
