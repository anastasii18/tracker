package main

import (
	"batching/pkg/app"
	"context"
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
	var batchSize string
	var err error

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

	config.BatchSize, err = strconv.Atoi(batchSize)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
