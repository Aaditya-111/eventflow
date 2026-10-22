package security

import (
	"context"
	"errors"
	"net/netip"
	"testing"
)

type fakeDNSResolver struct {
	ips []netip.Addr
	err error
}

func (f fakeDNSResolver) LookupNetIP(
	_ context.Context,
	_, _ string,
) ([]netip.Addr, error) {
	return f.ips, f.err
}

func TestValidateWebhookURL(t *testing.T) {
	publicIP := netip.MustParseAddr("8.8.8.8")
	privateIP := netip.MustParseAddr("192.168.1.10")

	tests := []struct {
		name         string
		url          string
		resolver     DNSResolver
		wantErr      bool
		wantUnsafe   bool
		wantDNSError bool
	}{
		{
			name:     "public HTTPS hostname",
			url:      "https://hooks.example.com/events",
			resolver: fakeDNSResolver{ips: []netip.Addr{publicIP}},
		},
		{
			name:       "HTTP rejected",
			url:        "http://hooks.example.com/events",
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "localhost rejected",
			url:        "https://localhost/events",
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "private literal rejected",
			url:        "https://192.168.1.10/events",
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "loopback literal rejected",
			url:        "https://127.0.0.1/events",
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "IPv6 loopback rejected",
			url:        "https://[::1]/events",
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "IPv6 unique local address rejected",
			url:        "https://[fc00::1]/events",
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "credentials rejected",
			url:        "https://user:pass@example.com/events",
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "fragment rejected",
			url:        "https://hooks.example.com/events#fragment",
			resolver:   fakeDNSResolver{ips: []netip.Addr{publicIP}},
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "malformed URL rejected",
			url:        "https://[invalid/events",
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:       "private DNS answer rejected",
			url:        "https://hooks.example.com/events",
			resolver:   fakeDNSResolver{ips: []netip.Addr{privateIP}},
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name: "mixed public and private DNS answers rejected",
			url:  "https://hooks.example.com/events",
			resolver: fakeDNSResolver{
				ips: []netip.Addr{publicIP, privateIP},
			},
			wantErr:    true,
			wantUnsafe: true,
		},
		{
			name:         "DNS failure is classified separately",
			url:          "https://hooks.example.com/events",
			resolver:     fakeDNSResolver{err: context.DeadlineExceeded},
			wantErr:      true,
			wantDNSError: true,
		},
		{
			name:         "empty DNS response is classified separately",
			url:          "https://hooks.example.com/events",
			resolver:     fakeDNSResolver{},
			wantErr:      true,
			wantDNSError: true,
		},
		{
			name:    "nil resolver is configuration error",
			url:     "https://hooks.example.com/events",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateWebhookURL(
				context.Background(),
				tt.url,
				tt.resolver,
			)

			if (err != nil) != tt.wantErr {
				t.Fatalf(
					"ValidateWebhookURL() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
			}

			if tt.wantUnsafe && !errors.Is(err, ErrUnsafeWebhookURL) {
				t.Fatalf("error = %v, want ErrUnsafeWebhookURL", err)
			}

			if tt.wantDNSError && !errors.Is(err, ErrWebhookDNSFailure) {
				t.Fatalf("error = %v, want ErrWebhookDNSFailure", err)
			}

			if tt.wantUnsafe && errors.Is(err, ErrWebhookDNSFailure) {
				t.Fatal("unsafe URL error must not be classified as a DNS failure")
			}
		})
	}
}
