package security

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
)

type transportTestResolver struct {
	ips      []netip.Addr
	err      error
	calls    int
	lastHost string
	lastNet  string
}

func (r *transportTestResolver) LookupNetIP(
	_ context.Context,
	network string,
	host string,
) ([]netip.Addr, error) {
	r.calls++
	r.lastHost = host
	r.lastNet = network

	return r.ips, r.err
}

type transportTestDialer struct {
	calls       int
	lastNetwork string
	lastAddress string
	err         error
}

func (d *transportTestDialer) DialContext(
	_ context.Context,
	network string,
	address string,
) (net.Conn, error) {
	d.calls++
	d.lastNetwork = network
	d.lastAddress = address

	if d.err != nil {
		return nil, d.err
	}

	return nil, errors.New("unexpected dial: test dialer has no connection")
}

func newTestWebhookTransport(
	resolver webhookIPResolver,
	dialer webhookTCPDialer,
) func(context.Context, string, string) (net.Conn, error) {
	client, err := newWebhookHTTPClient(
		5_000_000_000,
		webhookTransportConfig{
			resolver: resolver,
			dialer:   dialer,
		},
	)
	if err != nil {
		panic(err)
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		panic("expected *http.Transport")
	}

	return transport.DialContext
}

func TestWebhookTransportRejectsPrivateDNSBeforeDial(t *testing.T) {
	resolver := &transportTestResolver{
		ips: []netip.Addr{netip.MustParseAddr("127.0.0.1")},
	}
	dialer := &transportTestDialer{}

	dial := newTestWebhookTransport(resolver, dialer)
	_, err := dial(context.Background(), "tcp", "hooks.example.com:443")

	if err == nil {
		t.Fatal("expected private DNS result to be rejected")
	}
	if !strings.Contains(err.Error(), "non-public") {
		t.Fatalf("unexpected error: %v", err)
	}
	if dialer.calls != 0 {
		t.Fatalf("dialer called %d times; want 0", dialer.calls)
	}
}

func TestWebhookTransportRejectsMixedPublicAndPrivateDNS(t *testing.T) {
	resolver := &transportTestResolver{
		ips: []netip.Addr{
			netip.MustParseAddr("8.8.8.8"),
			netip.MustParseAddr("10.0.0.5"),
		},
	}
	dialer := &transportTestDialer{}

	dial := newTestWebhookTransport(resolver, dialer)
	_, err := dial(context.Background(), "tcp", "hooks.example.com:443")

	if err == nil {
		t.Fatal("expected mixed public/private DNS results to be rejected")
	}
	if dialer.calls != 0 {
		t.Fatalf("dialer called %d times; want 0", dialer.calls)
	}
}

func TestWebhookTransportRejectsDNSFailure(t *testing.T) {
	resolver := &transportTestResolver{
		err: errors.New("simulated DNS failure"),
	}
	dialer := &transportTestDialer{}

	dial := newTestWebhookTransport(resolver, dialer)
	_, err := dial(context.Background(), "tcp", "hooks.example.com:443")

	if err == nil {
		t.Fatal("expected DNS failure")
	}
	if dialer.calls != 0 {
		t.Fatalf("dialer called %d times; want 0", dialer.calls)
	}
}

func TestWebhookTransportRejectsEmptyDNSResults(t *testing.T) {
	resolver := &transportTestResolver{}
	dialer := &transportTestDialer{}

	dial := newTestWebhookTransport(resolver, dialer)
	_, err := dial(context.Background(), "tcp", "hooks.example.com:443")

	if err == nil {
		t.Fatal("expected empty DNS result to be rejected")
	}
	if dialer.calls != 0 {
		t.Fatalf("dialer called %d times; want 0", dialer.calls)
	}
}

func TestWebhookTransportRejectsInvalidDestinationPort(t *testing.T) {
	resolver := &transportTestResolver{}
	dialer := &transportTestDialer{}

	dial := newTestWebhookTransport(resolver, dialer)

	for _, address := range []string{
		"hooks.example.com:0",
		"hooks.example.com:65536",
		"hooks.example.com:notaport",
	} {
		t.Run(address, func(t *testing.T) {
			_, err := dial(context.Background(), "tcp", address)
			if err == nil {
				t.Fatal("expected invalid port to be rejected")
			}
		})
	}

	if resolver.calls != 0 || dialer.calls != 0 {
		t.Fatal("invalid port unexpectedly reached resolver or dialer")
	}
}
