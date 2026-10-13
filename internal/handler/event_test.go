package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Aaditya-111/eventflow/internal/repository"
)

type fakeEventCreator struct {
	events map[string]repository.Event
}

func newFakeEventCreator() *fakeEventCreator {
	return &fakeEventCreator{
		events: make(map[string]repository.Event),
	}
}

func (f *fakeEventCreator) Create(
	_ context.Context,
	event repository.Event,
) (bool, error) {
	if event.EventID == "" || event.Type == "" ||
		len(event.Data) == 0 || !json.Valid(event.Data) {
		return false, repository.ErrInvalidEvent
	}

	existing, found := f.events[event.EventID]
	if found {
		var incomingData, existingData any

		if err := json.Unmarshal(event.Data, &incomingData); err != nil {
			return false, repository.ErrInvalidEvent
		}
		if err := json.Unmarshal(existing.Data, &existingData); err != nil {
			return false, errors.New("invalid stored test data")
		}

		incomingJSON, err := json.Marshal(incomingData)
		if err != nil {
			return false, err
		}
		existingJSON, err := json.Marshal(existingData)
		if err != nil {
			return false, err
		}

		if existing.Type != event.Type ||
			string(incomingJSON) != string(existingJSON) {
			return false, repository.ErrEventConflict
		}

		return false, nil
	}

	f.events[event.EventID] = event
	return true, nil
}

func sendEvent(
	handler http.Handler,
	method string,
	body string,
) *httptest.ResponseRecorder {
	req := httptest.NewRequest(
		method,
		"/v1/events",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestCreateEventAccepted(t *testing.T) {
	repo := newFakeEventCreator()
	handler := CreateEventHandler(repo)

	body := `{
		"event_id": "evt_1",
		"type": "user.created",
		"data": {"user_id": "u_123"}
	}`

	rec := sendEvent(handler, http.MethodPost, body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusAccepted, rec.Code, rec.Body.String())
	}

	if len(repo.events) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(repo.events))
	}
}

func TestIdenticalEventIsIdempotent(t *testing.T) {
	repo := newFakeEventCreator()
	handler := CreateEventHandler(repo)

	body := `{
		"event_id": "evt_1",
		"type": "user.created",
		"data": {"user_id": "u_123"}
	}`

	first := sendEvent(handler, http.MethodPost, body)
	second := sendEvent(handler, http.MethodPost, body)

	if first.Code != http.StatusAccepted ||
		second.Code != http.StatusAccepted {
		t.Fatalf("expected both requests to return 202; got %d and %d",
			first.Code, second.Code)
	}

	if len(repo.events) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(repo.events))
	}

	var response eventResponse
	if err := json.Unmarshal(second.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode second response: %v", err)
	}

	if response.Message != "Identical event already accepted" {
		t.Fatalf("unexpected duplicate message: %q", response.Message)
	}
}

func TestSameIDDifferentPayloadConflicts(t *testing.T) {
	repo := newFakeEventCreator()
	handler := CreateEventHandler(repo)

	firstBody := `{
		"event_id": "evt_1",
		"type": "user.created",
		"data": {"plan": "pro"}
	}`
	secondBody := `{
		"event_id": "evt_1",
		"type": "user.created",
		"data": {"plan": "enterprise"}
	}`

	first := sendEvent(handler, http.MethodPost, firstBody)
	second := sendEvent(handler, http.MethodPost, secondBody)

	if first.Code != http.StatusAccepted {
		t.Fatalf("expected first request 202, got %d", first.Code)
	}
	if second.Code != http.StatusConflict {
		t.Fatalf("expected second request 409, got %d: %s",
			second.Code, second.Body.String())
	}

	if len(repo.events) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(repo.events))
	}
}

func TestInvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "malformed JSON",
			body: `{"event_id":`,
		},
		{
			name: "missing event ID",
			body: `{"type":"user.created","data":{"id":1}}`,
		},
		{
			name: "missing type",
			body: `{"event_id":"evt_1","data":{"id":1}}`,
		},
		{
			name: "missing data",
			body: `{"event_id":"evt_1","type":"user.created"}`,
		},
		{
			name: "null data",
			body: `{"event_id":"evt_1","type":"user.created","data":null}`,
		},
		{
			name: "unknown field",
			body: `{"event_id":"evt_1","type":"user.created","data":{},"extra":true}`,
		},
		{
			name: "trailing JSON",
			body: `{"event_id":"evt_1","type":"user.created","data":{}} {}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := CreateEventHandler(newFakeEventCreator())
			rec := sendEvent(handler, http.MethodPost, tt.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status 400, got %d: %s",
					rec.Code, rec.Body.String())
			}
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	handler := CreateEventHandler(newFakeEventCreator())

	rec := sendEvent(handler, http.MethodGet, "")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", rec.Code)
	}

	if rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("expected Allow: POST, got %q",
			rec.Header().Get("Allow"))
	}
}

func TestOversizedRequest(t *testing.T) {
	handler := CreateEventHandler(newFakeEventCreator())

	// The handler limit is 1 MiB. This body exceeds it.
	body := `{"event_id":"evt_1","type":"test","data":{"value":"` +
		strings.Repeat("a", (1<<20)+100) + `"}}`

	rec := sendEvent(handler, http.MethodPost, body)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s",
			rec.Code, rec.Body.String())
	}
}
