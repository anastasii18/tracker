package service

import (
	"context"
)

type IngestionService interface {
	BatchEvent(context context.Context, event Event, userAgent, remoteAddr string) error
}

var _ IngestionService = (*Service)(nil)

type Service struct {
	Publisher Publisher
}

func NewService(publisher Publisher) *Service {
	return &Service{Publisher: publisher}
}

func (s Service) BatchEvent(ctx context.Context, event Event, userAgent, remoteAddr string) error {
	err := s.Publisher.Publish(ctx, event, userAgent, remoteAddr)
	if err != nil {
		return err
	}

	return nil
}
