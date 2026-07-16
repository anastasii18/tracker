package db

import (
	"encoding/json"
	"time"
)

type Event struct {
	EventType      EventType
	UserID         string
	SessionID      string
	TargetID       string
	MetaData       json.RawMessage
	ClientTime     time.Time
	VisitorID      string
	OS             string
	Browser        string
	Device         string
	CountryIsoCode string
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
