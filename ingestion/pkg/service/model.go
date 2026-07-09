package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Event struct {
	EventType  EventType       `json:"event_type"`
	UserID     string          `json:"user_id"`
	SessionID  string          `json:"session_id"`
	TargetID   string          `json:"target_id"`
	MetaData   json.RawMessage `json:"metadata"`
	ClientTime time.Time       `json:"client_time"`
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

func (e *EventType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	s = strings.ToUpper(strings.TrimSpace(s))

	switch s {
	case "VACANCY_VIEW":
		*e = vacancy_view
	case "VACANCY_APPLY":
		*e = vacancy_apply
	case "VACANCY_SKIP":
		*e = vacancy_skip
	case "VACANCY_SAVE":
		*e = vacancy_save
	case "SEARCH_QUERY":
		*e = search_query
	case "RESUME_VIEW":
		*e = resume_view
	default:
		return fmt.Errorf("unknown  event type: %q", s)
	}
	return nil
}
