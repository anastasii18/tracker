package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DB struct {
	driver.Conn
}

type Config struct {
	URL      string
	Database string
	Username string
	Password string
}

func NewDB(ctx context.Context, config *Config) (*DB, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.URL},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
	})

	log.Println("Connected to ClickHouse")
	if err != nil {
		return nil, err
	}

	database := &DB{conn}

	err = database.InitEvents(ctx)
	if err != nil {
		return nil, err
	}

	return database, nil
}

func (db *DB) InitEvents(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS events (
			event_type Int32,
			user_id String,
			session_id String,
			target_id String,
			meta_data String,
			client_time DateTime,
			user_agent String,
			ip String
		) Engine = MergeTree()
		ORDER BY (event_type, client_time)
	`)

	return err
}

func (db *DB) SendBatch(ctx context.Context, batchData [][]byte) error {
	batch, err := db.Conn.PrepareBatch(ctx, "INSERT INTO events")
	if err != nil {
		return err
	}

	// Если Send() не отработает успешно, Abort() отменит ожидание ClickHouse.
	defer func() {
		_ = batch.Abort()
	}()

	for i := 0; i < len(batchData); i++ {
		var event Event
		err := json.Unmarshal(batchData[i], &event)
		if err != nil {
			log.Println(fmt.Errorf("ошибка расшифровки event: %w", err))
			continue
		}

		err = batch.Append(
			event.EventType,
			event.UserID,
			event.SessionID,
			event.TargetID,
			string(event.MetaData),
			event.ClientTime,
			event.UserAgent,
			event.IP)

		if err != nil {
			return err
		}
	}

	return batch.Send()
}
