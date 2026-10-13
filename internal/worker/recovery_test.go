package worker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestConsumerReclaimsPendingMessage(t *testing.T) {
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
	stream := "eventflow:test:recovery:" + suffix
	group := "recovery-group-" + suffix

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, stream).Err()
	}()

	first := newConsumerForStream(client, stream, group, "original-consumer")
	if err := first.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	id, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"event_id": "recovery-test-" + suffix,
			"type":     "user.created",
			"data":     `{"user_id":"recovery-test"}`,
		},
	}).Result()
	if err != nil {
		t.Fatalf("add test message: %v", err)
	}

	delivered, err := first.ReadNew(ctx, 1, 1000)
	if err != nil {
		t.Fatalf("read message as original consumer: %v", err)
	}
	if len(delivered) != 1 || delivered[0].ID != id {
		t.Fatalf("original consumer did not receive expected message")
	}

	recovery := newConsumerForStream(client, stream, group, "recovery-consumer")
	reclaimed, _, err := recovery.ReclaimPending(ctx, "0-0", 10, 0)
	if err != nil {
		t.Fatalf("ReclaimPending: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("reclaimed %d messages; want 1", len(reclaimed))
	}
	if reclaimed[0].ID != id {
		t.Fatalf("reclaimed ID = %q; want %q", reclaimed[0].ID, id)
	}
	if reclaimed[0].Values["event_id"] != "recovery-test-"+suffix {
		t.Fatalf("unexpected reclaimed event_id: %v", reclaimed[0].Values["event_id"])
	}

	// Acknowledge the test message so the test leaves no pending work.
	if err := recovery.Ack(ctx, id); err != nil {
		t.Fatalf("acknowledge reclaimed test message: %v", err)
	}

	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatalf("check pending messages: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count = %d after ACK; want 0", pending.Count)
	}
}
