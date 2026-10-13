package worker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisDeadLetterSinkPublishesToRealRedis(t *testing.T) {
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
	message := StreamMessage{
		ID: "source-test-" + suffix,
		Values: map[string]interface{}{
			"event_id": "dead-letter-test-" + suffix,
			"type":     "test.failure",
			"data":     `{"test":true}`,
		},
	}

	sink := NewRedisDeadLetterSink(client)
	if err := sink.Publish(ctx, message, 5, "integration test failure"); err != nil {
		t.Fatalf("publish dead-letter message: %v", err)
	}

	entries, err := client.XRevRangeN(
		ctx, DeadLetterStream, "+", "-", 100,
	).Result()
	if err != nil {
		t.Fatalf("read dead-letter stream: %v", err)
	}

	var found *redis.XMessage
	for i := range entries {
		if entries[i].Values["source_message_id"] == message.ID {
			found = &entries[i]
			break
		}
	}
	if found == nil {
		t.Fatal("test message was not found in the dead-letter stream")
	}

	// Remove only this test's entry; never delete the stream itself.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_ = client.XDel(cleanupCtx, DeadLetterStream, found.ID).Err()
	}()

	if found.Values["event_id"] != message.Values["event_id"] {
		t.Fatalf("event_id = %v; want %v",
			found.Values["event_id"], message.Values["event_id"])
	}
	if fmt.Sprint(found.Values["attempts"]) != "5" {
		t.Fatalf("attempts = %v; want 5", found.Values["attempts"])
	}
	if found.Values["failure_reason"] != "integration test failure" {
		t.Fatalf("unexpected failure_reason: %v", found.Values["failure_reason"])
	}
}
