package app

import (
	"batching/pkg/db"
	"batching/pkg/service"
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type diContainer struct {
	nats       *nats.Conn
	subscriber service.Subscriber
	db         *db.DB
}

func NewDiContainer() *diContainer {
	return &diContainer{}
}

func (d *diContainer) NewNats(ctx context.Context, config *Config) (*nats.Conn, error) {
	if d.nats == nil {
		opts := []nats.Option{
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(5),
			nats.ReconnectWait(2 * time.Second),
		}
		nc, err := nats.Connect(config.NatsUrl, opts...)
		if err != nil {
			return nil, err
		}

		d.nats = nc
	}

	return d.nats, nil
}

func (d *diContainer) NewSubscriber(ctx context.Context, config *Config) (service.Subscriber, error) {
	if d.subscriber == nil {
		nc, err := d.NewNats(ctx, config)
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
