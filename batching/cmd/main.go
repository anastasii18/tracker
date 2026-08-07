package main

import (
	"batching/pkg/app"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	ctx := context.Background()

	var a *app.App
	config, err := initConfig()
	if err != nil {
		log.Fatal("failed to init config: ", err)
	}

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
	var batchSize string

	secretsMapping := map[string]*string{
		"NATS_URL":             &config.NatsUrl,
		"CLICKHOUSE_URL":       &config.CLickHouseUrl,
		"CLICKHOUSE_PASSWORD":  &config.CLickHousePassword,
		"CLICKHOUSE_DB":        &config.CLickHouseDatabase,
		"CLICKHOUSE_USER_NAME": &config.CLickHouseUserName,
		"BATCH_SIZE":           &batchSize,
	}
	for key, target := range secretsMapping {
		*target = os.Getenv(key)
	}

	if batchSize == "" {
		config.BatchSize = 1000
	} else {
		parsed, err := strconv.Atoi(batchSize)
		if err != nil {
			return nil, fmt.Errorf("invalid BATCH_SIZE environment variable (must be integer): %w", err)
		}
		config.BatchSize = parsed
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("batching config validation failed: %w", err)
	}

	return &config, nil
}
