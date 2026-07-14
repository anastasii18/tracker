package main

import (
	"context"
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

	err = a.Run(ctx)
	if err != nil {
		log.Println("Ошибка при работе приложения")
		return
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Завершение работы сервера...")

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

	return &config, nil
}
