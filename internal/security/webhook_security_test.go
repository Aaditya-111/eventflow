package security

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewWebhookHTTPClientRejectsInvalidTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{name: "zero timeout", timeout: 0},
		{name: "negative timeout", timeout: -1 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewWebhookHTTPClient(tt.timeout)

			if err == nil {
				t.Fatal("expected an error for invalid timeout")
			}
			if client != nil {
				t.Fatal("expected a nil client for invalid timeout")
			}
		})
	}
}

func TestNewWebhookHTTPClientConfiguration(t *testing.T) {
	client, err := NewWebhookHTTPClient(10 * time.Second)
	if err != nil {
		t.Fatalf("NewWebhookHTTPClient() error = %v", err)
	}

	if client.Timeout != 10*time.Second {
		t.Fatalf("client timeout = %v, want %v",
			client.Timeout, 10*time.Second)
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("client transport is not *http.Transport")
	}

	if transport.Proxy != nil {
		t.Fatal("expected proxy to be disabled")
	}

	if transport.TLSClientConfig == nil {
		t.Fatal("expected explicit TLS configuration")
	}

	if transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minimum TLS version = %v, want TLS 1.2",
			transport.TLSClientConfig.MinVersion)
	}

	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("TLS certificate verification must remain enabled")
	}

	if transport.DialContext == nil {
		t.Fatal("expected a custom DNS-checking dialer")
	}

	if client.CheckRedirect == nil {
		t.Fatal("expected redirects to be explicitly controlled")
	}
}

func TestWebhookHTTPClientDoesNotFollowRedirects(t *testing.T) {
	redirectHit := false

	redirectServer := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			redirectHit = true
			w.WriteHeader(http.StatusNoContent)
		},
	))
	defer redirectServer.Close()

	// Use a local test server only to test redirect policy. The request
	// uses a custom transport with no network dialer so it cannot connect.
	client, err := NewWebhookHTTPClient(2 * time.Second)
	if err != nil {
		t.Fatalf("NewWebhookHTTPClient() error = %v", err)
	}

	client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusFound,
			Header: http.Header{
				"Location": []string{redirectServer.URL},
			},
			Body:    http.NoBody,
			Request: r,
		}, nil
	})

	resp, err := client.Get("https://hooks.example.com/events")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want %d",
			resp.StatusCode, http.StatusFound)
	}

	if redirectHit {
		t.Fatal("client followed a redirect unexpectedly")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
