package worker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
	"time"
)

func testSignature(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)

	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyWebhookSignature(t *testing.T) {
	secret := []byte("test-secret-for-unit-tests-only")
	timestamp := "1792152000"
	body := []byte(`{"event_id":"evt-test","type":"order.created"}`)

	validSignature := testSignature(secret, timestamp, body)

	tests := []struct {
		name      string
		secret    []byte
		timestamp string
		body      []byte
		signature string
		want      bool
	}{
		{
			name:      "valid signature",
			secret:    secret,
			timestamp: timestamp,
			body:      body,
			signature: validSignature,
			want:      true,
		},
		{
			name:      "wrong secret",
			secret:    []byte("different-test-secret"),
			timestamp: timestamp,
			body:      body,
			signature: validSignature,
			want:      false,
		},
		{
			name:      "modified body",
			secret:    secret,
			timestamp: timestamp,
			body:      []byte(`{"event_id":"evt-tampered"}`),
			signature: validSignature,
			want:      false,
		},
		{
			name:      "modified timestamp",
			secret:    secret,
			timestamp: "1792152001",
			body:      body,
			signature: validSignature,
			want:      false,
		},
		{
			name:      "malformed signature",
			secret:    secret,
			timestamp: timestamp,
			body:      body,
			signature: "sha256=not-hex",
			want:      false,
		},
		{
			name:      "missing prefix",
			secret:    secret,
			timestamp: timestamp,
			body:      body,
			signature: hex.EncodeToString([]byte("not a valid HMAC")),
			want:      false,
		},
		{
			name:      "empty secret",
			secret:    nil,
			timestamp: timestamp,
			body:      body,
			signature: validSignature,
			want:      false,
		},
		{
			name:      "empty timestamp",
			secret:    secret,
			timestamp: "",
			body:      body,
			signature: validSignature,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VerifyWebhookSignature(
				tt.secret,
				tt.timestamp,
				tt.body,
				tt.signature,
			)

			if got != tt.want {
				t.Fatalf("VerifyWebhookSignature() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerifyWebhookSignatureRejectsOversizedBody(t *testing.T) {
	secret := []byte("test-secret")
	timestamp := "1792152000"
	body := make([]byte, maxWebhookRequestBytes+1)
	signature := testSignature(secret, timestamp, body)

	if VerifyWebhookSignature(secret, timestamp, body, signature) {
		t.Fatal("expected oversized body to be rejected")
	}
}

func TestVerifyWebhookSignatureRejectsModifiedSignature(t *testing.T) {
	secret := []byte("test-secret")
	timestamp := "1792152000"
	body := []byte(`{"type":"order.created"}`)
	signature := testSignature(secret, timestamp, body)

	// Alter one hexadecimal character while keeping the header well-formed.
	last := signature[len(signature)-1]
	if last == '0' {
		signature = signature[:len(signature)-1] + "1"
	} else {
		signature = signature[:len(signature)-1] + "0"
	}

	if VerifyWebhookSignature(secret, timestamp, body, signature) {
		t.Fatal("expected modified signature to be rejected")
	}
}

func TestVerifyWebhookTimestamp(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		timestamp string
		want      bool
	}{
		{
			name:      "current timestamp",
			timestamp: strconv.FormatInt(now.Unix(), 10),
			want:      true,
		},
		{
			name:      "timestamp within allowed past window",
			timestamp: strconv.FormatInt(now.Add(-4*time.Minute).Unix(), 10),
			want:      true,
		},
		{
			name:      "timestamp within allowed future skew",
			timestamp: strconv.FormatInt(now.Add(4*time.Minute).Unix(), 10),
			want:      true,
		},
		{
			name:      "stale timestamp",
			timestamp: strconv.FormatInt(now.Add(-6*time.Minute).Unix(), 10),
			want:      false,
		},
		{
			name:      "timestamp too far in future",
			timestamp: strconv.FormatInt(now.Add(6*time.Minute).Unix(), 10),
			want:      false,
		},
		{
			name:      "empty timestamp",
			timestamp: "",
			want:      false,
		},
		{
			name:      "malformed timestamp",
			timestamp: "not-a-timestamp",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VerifyWebhookTimestamp(tt.timestamp, now)
			if got != tt.want {
				t.Fatalf("VerifyWebhookTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}
