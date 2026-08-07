package main

import (
	"context"
	"fmt"
	"ingestion/pkg/app"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	// Таймауты для HTTP-сервера
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 10 * time.Second
)

func main() {
	ctx := context.Background()
	config, err := initConfig()
	if err != nil {
		log.Fatal("failed to init config: ", err)
	}

	config.ReadHeaderTimeout = readHeaderTimeout
	config.ShutdownTimeout = shutdownTimeout

	var a *app.App
	a, err = app.New(ctx, config)
	if err != nil {
		log.Println("Ошибка при создании приложения")
		log.Fatal(err)
		return
	}

	// Graceful shutdown: ждём сигнал или завершение Run() без блокировки
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx) }()

	select {
	case err := <-errCh:
		if err != nil {
			log.Println("Ошибка при работе приложения")
			return
		}
	case <-quit:
		log.Println("Завершение работы сервера...")
	}

	a.Stop()
}

func initConfig() (*app.Config, error) {
	var config app.Config

	secretsMapping := map[string]*string{
		"HTTP_PORT":  &config.HttpPort,
		"HTTP_HOST":  &config.HttpHost,
		"NATS_URL":   &config.NatsUrl,
		"APP_SECRET": &config.AppSecret,
		"GEOIP_PATH": &config.GeoipPath,
	}
	for key, target := range secretsMapping {
		*target = os.Getenv(key)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &config, nil
}
