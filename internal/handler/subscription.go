package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Aaditya-111/eventflow/internal/model"
	"github.com/Aaditya-111/eventflow/internal/security"
	"github.com/Aaditya-111/eventflow/internal/service"
)

const maxSubscriptionBodyBytes = 16 << 10

type subscriptionCreator interface {
	Create(
		ctx context.Context,
		sub model.WebhookSubscription,
		plaintextSecret []byte,
	) error
}

type createSubscriptionRequest struct {
	SubscriptionID string   `json:"subscription_id"`
	EndpointURL    string   `json:"endpoint_url"`
	EventTypes     []string `json:"event_types"`
	Secret         string   `json:"secret"`
}

type createSubscriptionResponse struct {
	Message        string `json:"message"`
	SubscriptionID string `json:"subscription_id"`
}

func CreateSubscriptionHandler(svc subscriptionCreator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}

		if svc == nil {
			writeError(w, http.StatusServiceUnavailable, "subscription_service_unavailable")
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxSubscriptionBodyBytes)
		defer r.Body.Close()

		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()

		var input createSubscriptionRequest
		if err := decoder.Decode(&input); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeError(w, http.StatusRequestEntityTooLarge, "request_too_large")
				return
			}
			writeError(w, http.StatusBadRequest, "invalid_json")
			return
		}

		var extra json.RawMessage
		if err := decoder.Decode(&extra); err != io.EOF {
			writeError(w, http.StatusBadRequest, "trailing_json")
			return
		}

		input.SubscriptionID = strings.TrimSpace(input.SubscriptionID)
		input.EndpointURL = strings.TrimSpace(input.EndpointURL)
		input.Secret = strings.TrimSpace(input.Secret)

		if input.SubscriptionID == "" || len(input.SubscriptionID) > 200 {
			writeError(w, http.StatusBadRequest, "invalid_subscription_id")
			return
		}

		if input.EndpointURL == "" || len(input.EndpointURL) > 2048 {
			writeError(w, http.StatusBadRequest, "invalid_endpoint_url")
			return
		}

		if len(input.Secret) < 32 || len(input.Secret) > 256 {
			writeError(w, http.StatusBadRequest, "invalid_signing_secret")
			return
		}

		if len(input.EventTypes) == 0 || len(input.EventTypes) > 100 {
			writeError(w, http.StatusBadRequest, "invalid_event_types")
			return
		}
		validationCtx, validationCancel := context.WithTimeout(
			r.Context(),
			3*time.Second,
		)
		defer validationCancel()

		if err := security.ValidateWebhookURL(
			validationCtx,
			input.EndpointURL,
			security.NetDNSResolver{},
		); err != nil {
			writeError(w, http.StatusBadRequest, "unsafe_webhook_url")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		sub := model.WebhookSubscription{
			SubscriptionID: input.SubscriptionID,
			EndpointURL:    input.EndpointURL,
			EventTypes:     input.EventTypes,
			Active:         true,
		}

		err := svc.Create(ctx, sub, []byte(input.Secret))
		if err != nil {
			if errors.Is(err, service.ErrInvalidSubscription) {
				writeError(w, http.StatusBadRequest, "invalid_subscription")
				return
			}

			log.Printf("subscription creation failed: %v", err)
			writeError(w, http.StatusServiceUnavailable, "subscription_creation_failed")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(createSubscriptionResponse{
			Message:        "Subscription created",
			SubscriptionID: input.SubscriptionID,
		})
	}
}
