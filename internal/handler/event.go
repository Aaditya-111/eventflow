package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Aaditya-111/eventflow/internal/repository"
)

const maxEventBodyBytes = 1 << 20

type eventRequest struct {
	EventID string          `json:"event_id"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`
}

type eventResponse struct {
	Message string `json:"message"`
	EventID string `json:"event_id"`
	Type    string `json:"type"`
}

type eventCreator interface {
	Create(
		ctx context.Context,
		event repository.Event,
	) (bool, error)
}

func CreateEventHandler(repo eventCreator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxEventBodyBytes)
		defer r.Body.Close()

		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()

		var input eventRequest
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

		input.EventID = strings.TrimSpace(input.EventID)
		input.Type = strings.TrimSpace(input.Type)

		if input.EventID == "" || len(input.EventID) > 200 {
			writeError(w, http.StatusBadRequest, "invalid_event_id")
			return
		}
		if input.Type == "" || len(input.Type) > 200 {
			writeError(w, http.StatusBadRequest, "invalid_event_type")
			return
		}

		data := bytes.TrimSpace(input.Data)
		if len(data) == 0 ||
			bytes.Equal(data, []byte("null")) ||
			(data[0] != '{' && data[0] != '[') ||
			!json.Valid(data) {
			writeError(w, http.StatusBadRequest, "data_must_be_json_object_or_array")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		created, err := repo.Create(ctx, repository.Event{
			EventID: input.EventID,
			Type:    input.Type,
			Data:    data,
		})
		if err != nil {
			if errors.Is(err, repository.ErrInvalidEvent) {
				writeError(w, http.StatusBadRequest, "invalid_event")
				return
			}
			if errors.Is(err, repository.ErrEventConflict) {
				writeError(w, http.StatusConflict, "event_id_conflict")
				return
			}

			log.Printf("event persistence failed: %v", err)
			writeError(w, http.StatusServiceUnavailable, "event_storage_unavailable")
			return
		}

		message := "Event accepted"
		if !created {
			message = "Identical event already accepted"
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(eventResponse{
			Message: message,
			EventID: input.EventID,
			Type:    input.Type,
		})
	}
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}
