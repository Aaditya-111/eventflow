package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequireBearerToken(t *testing.T) {
	const expectedToken = "test-token-not-a-real-secret"

	tests := []struct {
		name            string
		configuredToken string
		authorization   string
		wantStatus      int
		wantNextCalled  bool
	}{
		{
			name:            "valid token",
			configuredToken: expectedToken,
			authorization:   "Bearer " + expectedToken,
			wantStatus:      http.StatusNoContent,
			wantNextCalled:  true,
		},
		{
			name:            "missing authorization",
			configuredToken: expectedToken,
			authorization:   "",
			wantStatus:      http.StatusUnauthorized,
			wantNextCalled:  false,
		},
		{
			name:            "wrong token",
			configuredToken: expectedToken,
			authorization:   "Bearer wrong-token",
			wantStatus:      http.StatusUnauthorized,
			wantNextCalled:  false,
		},
		{
			name:            "empty bearer token",
			configuredToken: expectedToken,
			authorization:   "Bearer ",
			wantStatus:      http.StatusUnauthorized,
			wantNextCalled:  false,
		},
		{
			name:            "wrong authorization scheme",
			configuredToken: expectedToken,
			authorization:   "Basic abc",
			wantStatus:      http.StatusUnauthorized,
			wantNextCalled:  false,
		},
		{
			name:            "token not configured",
			configuredToken: "",
			authorization:   "Bearer " + expectedToken,
			wantStatus:      http.StatusServiceUnavailable,
			wantNextCalled:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nextCalled := false

			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nextCalled = true
				w.WriteHeader(http.StatusNoContent)
			})

			protected := RequireBearerToken(tt.configuredToken, next)
			req := httptest.NewRequest(http.MethodPost, "/test", nil)

			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}

			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d: %s",
					tt.wantStatus, rec.Code, rec.Body.String())
			}

			if nextCalled != tt.wantNextCalled {
				t.Fatalf("expected next handler called = %v, got %v",
					tt.wantNextCalled, nextCalled)
			}

			if tt.wantStatus == http.StatusUnauthorized &&
				rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("expected WWW-Authenticate: Bearer, got %q",
					rec.Header().Get("WWW-Authenticate"))
			}
		})
	}
}
