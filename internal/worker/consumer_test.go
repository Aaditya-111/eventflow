package worker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestConsumerReadAndAck(t *testing.T) {
	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		t.Skip("integration test requires REDIS_PASSWORD from the project's existing environment")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to configured Redis: %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	stream := "eventflow:test:consumer:" + suffix
	group := "test-group-" + suffix
	consumerName := "test-consumer-" + suffix

	// Cleanup is limited to this test's unique stream.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, stream).Err()
	}()

	consumer := newConsumerForStream(client, stream, group, consumerName)

	if err := consumer.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	messageID, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"event_id": "test-" + suffix,
			"type":     "user.created",
			"data":     `{"user_id":"test-user"}`,
		},
	}).Result()
	if err != nil {
		t.Fatalf("add test message: %v", err)
	}

	messages, err := consumer.ReadNew(ctx, 1, 1000)
	if err != nil {
		t.Fatalf("ReadNew: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("ReadNew returned %d messages; want 1", len(messages))
	}
	if messages[0].ID != messageID {
		t.Fatalf("message ID = %q; want %q", messages[0].ID, messageID)
	}
	if messages[0].Values["event_id"] != "test-"+suffix {
		t.Fatalf("unexpected event_id: %v", messages[0].Values["event_id"])
	}
	if messages[0].Values["type"] != "user.created" {
		t.Fatalf("unexpected type: %v", messages[0].Values["type"])
	}

	if err := consumer.Ack(ctx, messageID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatalf("check pending messages: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count = %d after ACK; want 0", pending.Count)
	}
}
