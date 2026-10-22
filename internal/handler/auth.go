package handler

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

func RequireBearerToken(expectedToken string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expectedToken == "" {
			writeError(w, http.StatusServiceUnavailable, "authentication_not_configured")
			return
		}

		const prefix = "Bearer "
		header := r.Header.Get("Authorization")

		if !strings.HasPrefix(header, prefix) {
			w.Header().Set("WWW-Authenticate", `Bearer`)
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		provided := strings.TrimPrefix(header, prefix)
		if provided == "" ||
			subtle.ConstantTimeCompare(
				[]byte(provided),
				[]byte(expectedToken),
			) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		next.ServeHTTP(w, r)
	})
}
