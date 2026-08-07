package app

import (
	"context"
	"errors"
	"log"

	"go.uber.org/zap"
)

type Config struct {
	NatsUrl            string
	CLickHouseUrl      string
	CLickHouseDatabase string
	CLickHouseUserName string
	CLickHousePassword string
	BatchSize          int
}

type App struct {
	Config      *Config
	diContainer *diContainer
}

func New(ctx context.Context, config *Config) (*App, error) {
	app := &App{Config: config}

	err := app.initDI(ctx)
	if err != nil {
		return nil, err
	}

	_, err = app.diContainer.NewSubscriber(ctx, config)
	if err != nil {
		return nil, err
	}

	return app, nil
}

func (app *App) initDI(ctx context.Context) error {
	app.diContainer = NewDiContainer()
	return nil
}

func (app *App) Run(ctx context.Context) error {
	errCh := make(chan error, 2)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := app.diContainer.subscriber.Subscribe(ctx, app.Config.BatchSize)
	if err != nil {
		log.Println("Не удалось запустить подписчика:", err)
		return err
	}

	select {
	case <-ctx.Done():
		log.Println("Shutdown signal received")
	case err := <-errCh:
		log.Println("Component crashed, shutting down", zap.Error(err))
		cancel()
		<-ctx.Done()
		return err
	}

	return nil
}

func (app *App) Stop() {
	log.Println("Остановка сервиса Batching...")

	if app.diContainer.natsConn != nil {
		// Drain() корректно завершает подписки и закрывает коннект
		_ = app.diContainer.natsConn.Drain()
	}

	log.Println("Сервис Batching остановлен")
}

func (c *Config) Validate() error {
	if c.NatsUrl == "" {
		return errors.New("NATS_URL is required")
	}
	if c.CLickHouseUrl == "" {
		return errors.New("CLICKHOUSE_URL is required")
	}
	if c.CLickHouseDatabase == "" {
		return errors.New("CLICKHOUSE_DB is required")
	}
	if c.CLickHouseUserName == "" {
		return errors.New("CLICKHOUSE_USER_NAME is required")
	}

	if c.BatchSize <= 0 {
		return errors.New("BATCH_SIZE must be greater than 0")
	}

	return nil
}
