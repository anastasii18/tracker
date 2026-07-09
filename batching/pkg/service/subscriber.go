package service

import (
	"batching/pkg/db"
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

type Subscriber interface {
	Subscribe(ctx context.Context, batchSize int) error
}

var _ Subscriber = (*NcSubscriber)(nil)

type NcSubscriber struct {
	Nats     *nats.Conn
	Database *db.DB
}

func NewSubscriber(nc *nats.Conn, database *db.DB) *NcSubscriber {
	return &NcSubscriber{Nats: nc, Database: database}
}

func (ncs NcSubscriber) Subscribe(ctx context.Context, batchSize int) error {
	batchChan := make(chan []byte, batchSize)

	go ncs.runBatcher(ctx, batchChan, batchSize)

	// Подписываемся на все события, которые начинаются на hh.events
	_, err := ncs.Nats.Subscribe("hh.events", func(m *nats.Msg) {
		batchChan <- m.Data
		log.Printf("Получено событие %s из топика %s", m.Data, m.Subject)
	})

	if err != nil {
		return err
	}

	log.Println("Успешная подписка на топик hh.events")
	return nil
}

func (ncs *NcSubscriber) runBatcher(ctx context.Context, batchChan <-chan []byte, batchSize int) {
	// Делаем принудительный сброс батча каждые 5 секунд,
	// даже если 1000 сообщений не накопилось (может потом сделать)
	// ticker := time.NewTicker(5 * time.Second)
	// defer ticker.Stop()

	var batch [][]byte

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				ncs.flushToClickHouse(ctx, batch) // Спасаем остатки
			}
			return

		case msg := <-batchChan:
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				ncs.flushToClickHouse(ctx, batch)
				batch = nil // Обнуляем батч после отправки
			}
		}
	}
}

func (ncs *NcSubscriber) flushToClickHouse(ctx context.Context, batch [][]byte) {
	log.Printf("Отправка батча из %d записей в ClickHouse...", len(batch))

	err := ncs.Database.SendBatch(ctx, batch)

	if err != nil {
		log.Println(fmt.Errorf("ошибка отправки батча: %w", err))
		return
	}
}
