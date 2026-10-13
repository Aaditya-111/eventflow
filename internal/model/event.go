package model

import "encoding/json"

type Event struct {
	EventID string          `json:"event_id"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`
}