package app

import (
	"batching/pkg/db"
	"batching/pkg/service"
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type diContainer struct {
	natsConn      *nats.Conn
	natsJSContext nats.JetStreamContext
	subscriber    service.Subscriber
	db            *db.DB
}

func NewDiContainer() *diContainer {
	return &diContainer{}
}

func (d *diContainer) NewNatsConn(ctx context.Context, config *Config) (*nats.Conn, error) {
	if d.natsConn == nil {
		opts := []nats.Option{
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(5),
			nats.ReconnectWait(2 * time.Second),
		}
		nc, err := nats.Connect(config.NatsUrl, opts...)
		if err != nil {
			return nil, err
		}
		d.natsConn = nc
	}

	return d.natsConn, nil
}

func (d *diContainer) NewNatsJSContext(ctx context.Context, config *Config) (nats.JetStreamContext, error) {
	if d.natsJSContext == nil {
		nc, err := d.NewNatsConn(ctx, config)
		if err != nil {
			return nil, err
		}

		// лимит невыполненных асинхронных операций
		js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
		if err != nil {
			return nil, err
		}

		d.natsJSContext = js
	}

	return d.natsJSContext, nil
}

func (d *diContainer) NewSubscriber(ctx context.Context, config *Config) (service.Subscriber, error) {
	if d.subscriber == nil {
		nc, err := d.NewNatsJSContext(ctx, config)
		if err != nil {
			return nil, err
		}

		database, err := d.NewDB(ctx, config)
		if err != nil {
			return nil, err
		}

		d.subscriber = service.NewSubscriber(nc, database)
	}

	return d.subscriber, nil
}

func (d *diContainer) NewDB(ctx context.Context, config *Config) (*db.DB, error) {
	if d.db == nil {
		database, err := db.NewDB(ctx,
			&db.Config{
				URL:      config.CLickHouseUrl,
				Database: config.CLickHouseDatabase,
				Username: config.CLickHouseUserName,
				Password: config.CLickHousePassword,
			})
		if err != nil {
			return nil, err
		}
		d.db = database
	}

	return d.db, nil
}
