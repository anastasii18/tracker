package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

type Publisher interface {
	Publish(context context.Context, event Event, userAgent, remoteAddr string) error
}

var _ Publisher = (*NcPublisher)(nil)

type NcPublisher struct {
	*nats.Conn
}

func NewPublisher(nc *nats.Conn) *NcPublisher {
	return &NcPublisher{nc}
}

func (ncp NcPublisher) Publish(ctx context.Context, event Event, userAgent, remoteAddr string) error {
	batchingEvent := IngestionEventToBatchEvent(event, userAgent, remoteAddr)
	payload, err := json.Marshal(batchingEvent)

	if err != nil {
		log.Println(fmt.Errorf("ошибка расшифровки event: %w", err))
		return err
	}

	err = ncp.Conn.Publish("hh.events", payload)

	if err != nil {
		log.Println("Ошибка отправки в NATS:", err)
		return err
	}

	return nil
}
