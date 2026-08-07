package db

import (
	"encoding/json"
	"time"
)

type Event struct {
	EventId        string          `json:"event_id"`
	EventType      EventType       `json:"event_type"`
	UserID         string          `json:"user_id"`
	SessionID      string          `json:"session_id"`
	TargetID       string          `json:"target_id"`
	MetaData       json.RawMessage `json:"metadata"`
	ClientTime     time.Time       `json:"client_time"`
	VisitorID      string          `json:"visitor_id"`
	OS             string          `json:"os"`
	Browser        string          `json:"browser"`
	Device         string          `json:"device"`
	CountryIsoCode string          `json:"country_iso_code"`
}

type EventType int32

const (
	vacancy_view  EventType = 0
	vacancy_apply EventType = 1
	vacancy_skip  EventType = 2
	vacancy_save  EventType = 3
	search_query  EventType = 4
	resume_view   EventType = 5
)
