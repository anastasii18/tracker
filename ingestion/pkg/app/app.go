package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type Config struct {
	HttpPort          string
	HttpHost          string
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration
	NatsUrl           string
	AppSecret         string
	GeoipPath         string
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

	err = app.initServer(ctx)
	if err != nil {
		return nil, err
	}

	return app, nil
}

func (app *App) initDI(ctx context.Context) error {
	app.diContainer = NewDiContainer()
	return nil
}

func (app *App) initServer(ctx context.Context) error {
	_, err := app.diContainer.NewServer(ctx, app.Config)
	if err != nil {
		return err
	}

	go func() {
		log.Println(fmt.Sprintf("HTTP-сервер запущен на порту %s\n", app.Config.HttpPort))
		err := app.diContainer.Server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println(fmt.Sprintf("Ошибка запуска сервера: %v\n", err))
		}
	}()

	return nil
}

func (app *App) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.ShutdownTimeout)
	defer cancel()

	if err := app.diContainer.publisher.Drain(); err != nil {
		log.Println(err)
	}

	if app.diContainer.geoipDB != nil {
		err := app.diContainer.geoipDB.Close()
		if err != nil {
			log.Println(err)
		}
	}

	err := app.diContainer.Server.Shutdown(ctx)
	if err != nil {
		log.Println(fmt.Sprintf("Ошибка при остановке сервера: %v\n", err))
	}

	log.Println("Сервер остановлен")
}

func (c *Config) Validate() error {
	if c.HttpPort == "" {
		return errors.New("HTTP_PORT is required")
	}
	if c.NatsUrl == "" {
		return errors.New("NATS_URL is required")
	}
	if c.AppSecret == "" {
		return errors.New("APP_SECRET is required")
	}
	if c.GeoipPath == "" {
		return errors.New("GEOIP_PATH is required")
	}

	return nil
}
