package security

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

type webhookIPResolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

type webhookTCPDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type systemWebhookIPResolver struct{}

func (systemWebhookIPResolver) LookupNetIP(
	ctx context.Context,
	network string,
	host string,
) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, network, host)
}

type webhookTransportConfig struct {
	resolver webhookIPResolver
	dialer   webhookTCPDialer
}

func NewWebhookHTTPClient(timeout time.Duration) (*http.Client, error) {
	return newWebhookHTTPClient(timeout, webhookTransportConfig{
		resolver: systemWebhookIPResolver{},
		dialer: &net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		},
	})
}

func newWebhookHTTPClient(
	timeout time.Duration,
	config webhookTransportConfig,
) (*http.Client, error) {
	if timeout <= 0 {
		return nil, errors.New("webhook HTTP timeout must be positive")
	}
	if config.resolver == nil {
		return nil, errors.New("webhook DNS resolver is required")
	}
	if config.dialer == nil {
		return nil, errors.New("webhook TCP dialer is required")
	}

	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(
			ctx context.Context,
			network string,
			address string,
		) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, fmt.Errorf("parse webhook destination: %w", err)
			}

			host = strings.TrimSuffix(host, ".")
			if host == "" {
				return nil, errors.New("webhook destination host is empty")
			}

			portNumber, err := strconv.Atoi(port)
			if err != nil || portNumber < 1 || portNumber > 65535 {
				return nil, errors.New("invalid webhook destination port")
			}

			var addresses []netip.Addr
			if ip, parseErr := netip.ParseAddr(host); parseErr == nil {
				addresses = []netip.Addr{ip}
			} else {
				addresses, err = config.resolver.LookupNetIP(ctx, "ip", host)
				if err != nil {
					return nil, fmt.Errorf("resolve webhook host: %w", err)
				}
			}

			if len(addresses) == 0 {
				return nil, errors.New("webhook host has no IP addresses")
			}

			for _, ip := range addresses {
				if !isPublicIP(ip) {
					return nil, errors.New(
						"webhook host resolved to a non-public IP address",
					)
				}
			}

			var lastErr error
			for _, ip := range addresses {
				target := net.JoinHostPort(ip.String(), port)
				conn, dialErr := config.dialer.DialContext(
					ctx,
					network,
					target,
				)
				if dialErr == nil {
					return conn, nil
				}

				lastErr = dialErr
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
			}

			return nil, fmt.Errorf("connect to public webhook IP: %w", lastErr)
		},
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          20,
		MaxIdleConnsPerHost:   2,
		ForceAttemptHTTP2:     false,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(
			_ *http.Request,
			_ []*http.Request,
		) error {
			return http.ErrUseLastResponse
		},
	}, nil
}
