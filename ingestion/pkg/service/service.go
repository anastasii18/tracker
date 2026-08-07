package service

import (
	"context"
	"fmt"
)

type IngestionService interface {
	BatchEvent(context context.Context, event Event, userAgent, remoteAddr, appSecret string) error
}

var _ IngestionService = (*Service)(nil)

type Service struct {
	Publisher Publisher
}

func NewService(publisher Publisher) *Service {
	return &Service{Publisher: publisher}
}

func (s Service) BatchEvent(ctx context.Context, event Event, userAgent, remoteAddr, appSecret string) error {
	if event.UserID == "" {
		return &ValidationError{Field: "user_id"}
	}
	if event.SessionID == "" {
		return &ValidationError{Field: "session_id"}
	}
	if event.TargetID == "" {
		return &ValidationError{Field: "target_id"}
	}

	err := s.Publisher.Publish(ctx, event, userAgent, remoteAddr, appSecret)
	if err != nil {
		return err
	}

	return nil
}

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error on field %s: %s", e.Field, e.Message)
}

type InfrastructureError struct {
	Operation string
	Err       error
}

func (e *InfrastructureError) Error() string {
	return fmt.Sprintf("infrastructure error during %s: %v", e.Operation, e.Err)
}
