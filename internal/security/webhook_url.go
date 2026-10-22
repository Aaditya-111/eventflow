package security

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

var (
	ErrUnsafeWebhookURL  = errors.New("unsafe webhook URL")
	ErrWebhookDNSFailure = errors.New("webhook DNS lookup failed")
)

type DNSResolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

type NetDNSResolver struct{}

func (NetDNSResolver) LookupNetIP(
	ctx context.Context,
	network string,
	host string,
) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, network, host)
}

// ValidateWebhookURL permits HTTPS URLs only, with globally routable IP addresses.
// DNS is resolved during validation, but callers must revalidate at delivery time.
func ValidateWebhookURL(
	ctx context.Context,
	rawURL string,
	resolver DNSResolver,
) error {

	if len(rawURL) == 0 || len(rawURL) > 2048 {
		return fmt.Errorf("%w: invalid URL length", ErrUnsafeWebhookURL)
	}

	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return fmt.Errorf("%w: malformed URL", ErrUnsafeWebhookURL)
	}

	if u.Scheme != "https" || u.Hostname() == "" ||
		u.User != nil || u.Opaque != "" {
		return fmt.Errorf("%w: HTTPS URL without credentials is required", ErrUnsafeWebhookURL)
	}

	if strings.Contains(rawURL, "#") {
		return fmt.Errorf("%w: URL fragments are not allowed", ErrUnsafeWebhookURL)
	}

	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return fmt.Errorf("%w: prohibited hostname", ErrUnsafeWebhookURL)
	}

	// Literal IP addresses don't need DNS resolution.
	if ip, err := netip.ParseAddr(host); err == nil {
		if !isPublicIP(ip) {
			return fmt.Errorf("%w: non-public IP address", ErrUnsafeWebhookURL)
		}
		return nil
	}
	if resolver == nil {
		return errors.New("webhook DNS resolver is required")
	}

	ips, err := resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return fmt.Errorf("%w: lookup failed", ErrWebhookDNSFailure)
	}
	if len(ips) == 0 {
		return fmt.Errorf("%w: hostname has no IP addresses", ErrWebhookDNSFailure)
	}

	// Reject the hostname if even one returned address is unsafe.
	for _, ip := range ips {
		if !isPublicIP(ip) {
			return fmt.Errorf("%w: hostname resolves to a non-public IP address", ErrUnsafeWebhookURL)
		}
	}

	return nil
}

func isPublicIP(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}

	ip = ip.Unmap()

	// IsGlobalUnicast alone is insufficient: it includes private and some
	// special-purpose ranges. Explicitly reject those ranges.
	if !ip.IsGlobalUnicast() ||
		ip.IsPrivate() ||
		ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified() {
		return false
	}

	blocked := []netip.Prefix{
		netip.MustParsePrefix("100.64.0.0/10"),   // Shared address space
		netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
		netip.MustParsePrefix("192.0.2.0/24"),    // Documentation
		netip.MustParsePrefix("198.18.0.0/15"),   // Benchmarking
		netip.MustParsePrefix("198.51.100.0/24"), // Documentation
		netip.MustParsePrefix("203.0.113.0/24"),  // Documentation
		netip.MustParsePrefix("240.0.0.0/4"),     // Reserved IPv4
		netip.MustParsePrefix("2001:db8::/32"),   // IPv6 documentation
	}

	for _, prefix := range blocked {
		if prefix.Contains(ip) {
			return false
		}
	}

	return true
}
