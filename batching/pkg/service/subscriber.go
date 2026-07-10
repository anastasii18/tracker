package service

import (
	"batching/pkg/db"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type Subscriber interface {
	Subscribe(ctx context.Context, batchSize int) error
}

var _ Subscriber = (*NcSubscriber)(nil)

type NcSubscriber struct {
	NatsJSContext nats.JetStreamContext
	Database      *db.DB
}

func (ncs NcSubscriber) CreateStream(streamName, streamSubject string) error {
	stream, err := ncs.NatsJSContext.StreamInfo(streamName)

	cfg := &nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{streamSubject, streamSubject + ".dlq"},
		MaxAge:    7 * 24 * time.Hour, // Храним события не дольше 7 дней
		Retention: nats.LimitsPolicy,
	}

	if stream == nil {
		log.Printf("Creating stream: %s\n", streamName)
		_, err = ncs.NatsJSContext.AddStream(cfg)
		if err != nil {
			return err
		}
	} else {
		log.Printf("Updating stream: %s\n", streamName)
		_, err = ncs.NatsJSContext.UpdateStream(cfg)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewSubscriber(nc nats.JetStreamContext, database *db.DB) *NcSubscriber {
	return &NcSubscriber{NatsJSContext: nc, Database: database}
}

func (ncs NcSubscriber) Subscribe(ctx context.Context, batchSize int) error {
	err := ncs.CreateStream("events", "hh.events")
	if err != nil {
		return err
	}

	// Подписываемся на все события, которые начинаются на hh.events
	sub, err := ncs.NatsJSContext.PullSubscribe("hh.events", "batching-worker")
	if err != nil {
		return err
	}

	go ncs.runBatcher(ctx, sub, batchSize)

	log.Println("Успешная подписка на топик hh.events")
	return nil
}

func (ncs *NcSubscriber) runBatcher(ctx context.Context, sub *nats.Subscription, batchSize int) {
	batch := make([][]byte, 0, batchSize)
	messageRefs := make([]*nats.Msg, 0, batchSize)

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				err := ncs.flushToClickHouse(ctx, batch) // Спасаем остатки
				if err != nil {
					log.Println(fmt.Errorf("error run batcher (flush): %w", err))
					return
				}
			}
			return
		default:
		}

		// Запрашиваем батч
		msgs, err := sub.Fetch(batchSize, nats.MaxWait(15*time.Second))
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) {
				log.Printf("Ошибка получения батча: %v", err)
			}
			if len(batch) == 0 {
				continue
			}
		}

		for _, msg := range msgs {
			meta, _ := msg.Metadata()
			if int(meta.NumDelivered) > 5 {
				// Отправляем в DLQ и терминируем
				_, err := ncs.NatsJSContext.Publish("hh.events.dlq", msg.Data)
				if err != nil {
					log.Printf("ошибка записи в DLQ: %v\n", err)
					_ = msg.NakWithDelay(1 * time.Minute)
					continue
				}
				err = msg.Term()
				if err != nil {
					log.Println(fmt.Errorf("error run batcher (term): %w", err))
					return
				}
				continue
			}

			batch = append(batch, msg.Data)
			messageRefs = append(messageRefs, msg)
		}

		if len(batch) >= batchSize || errors.Is(err, nats.ErrTimeout) {
			// Отправка в ClickHouse
			err = ncs.flushToClickHouse(ctx, batch)
			if err != nil {
				log.Printf("Ошибка записи в CH, делаем Nak: %v", err)
				// Возвращаем все сообщения в очередь с задержкой (Backoff)
				for _, msg := range messageRefs {
					err := msg.NakWithDelay(5 * time.Second)
					if err != nil {
						log.Println(fmt.Errorf("error run batcher (nak): %w", err))
						return
					}
				}
				continue
			}

			// Если всё успешно — подтверждаем весь батч
			for _, msg := range messageRefs {
				err := msg.Ack()
				if err != nil {
					log.Println(fmt.Errorf("error run batcher (Ack): %w", err))
					return
				}
			}

			// Обнуляем после отправки
			batch = batch[:0]
			messageRefs = messageRefs[:0]
		}
	}
}

func (ncs *NcSubscriber) flushToClickHouse(ctx context.Context, batch [][]byte) error {
	log.Printf("Отправка батча из %d записей в ClickHouse...", len(batch))

	err := ncs.Database.SendBatch(ctx, batch)

	if err != nil {
		log.Println(fmt.Errorf("ошибка отправки батча: %w", err))
		return err
	}
	return nil
}
