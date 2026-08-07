package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cespare/xxhash/v2"
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

	err = database.InitSummingEvents(ctx)
	if err != nil {
		return nil, err
	}

	err = database.InitSummingEventsMV(ctx)
	if err != nil {
		return nil, err
	}

	err = database.InitAggregateEvents(ctx)
	if err != nil {
		return nil, err
	}

	err = database.InitAggregateEventsMV(ctx)
	if err != nil {
		return nil, err
	}

	return database, nil
}

func (db *DB) InitEvents(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS events (
		    event_id String,
			event_type Int32,
			user_id String,
			session_id String,
			target_id String,
			meta_data String,
			client_time DateTime,
			os LowCardinality(String),
		    browser LowCardinality(String),
		    device LowCardinality(String),
		    visitor_id  UInt64,  
		    country_iso_code LowCardinality(String),
		) Engine = MergeTree()
		PARTITION BY toYYYYMM(client_time)
		ORDER BY (event_type, client_time)
		TTL client_time + INTERVAL 90 DAY
		SETTINGS non_replicated_deduplication_window = 1000;
	`)

	return err
}

func (db *DB) InitSummingEvents(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS type_events_sum (
			event_type Int32,
			target_id String,
			date Date,
			clicks UInt64,
		) Engine = SummingMergeTree()
		ORDER BY (event_type, target_id, date)
		SETTINGS non_replicated_deduplication_window = 1000
	`)

	return err
}

func (db *DB) InitSummingEventsMV(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE MATERIALIZED VIEW IF NOT EXISTS type_events_sum_mv
		TO type_events_sum
		AS
		SELECT
			event_type,
			target_id,
			toDate(client_time) AS date,
			count() AS clicks
		FROM events
		GROUP BY event_type, target_id, toDate(client_time);
    `)

	return err
}

func (db *DB) InitAggregateEvents(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS events_visitor (
		    date Date,
		    visits AggregateFunction(count, UInt64),
		    users AggregateFunction(uniqCombined, UInt64),
		)
		ENGINE = AggregatingMergeTree() 
		ORDER BY date
		SETTINGS non_replicated_deduplication_window = 1000;
	`)

	return err
}

func (db *DB) InitAggregateEventsMV(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
		CREATE MATERIALIZED VIEW IF NOT EXISTS events_visitor_mv
		TO events_visitor
		AS
		SELECT
		toDate(client_time) AS date,
		uniqCombinedState(visitor_id) AS users,                                                                                                                        
		countState() AS visits   
		FROM events
		GROUP BY toDate(client_time);
    `)

	return err
}

func (db *DB) SendBatch(ctx context.Context, batchData [][]byte) error {
	batch, err := db.Conn.PrepareBatch(ctx, "INSERT INTO events SETTINGS "+
		"insert_deduplicate = 1,\n    "+
		"deduplicate_blocks_in_dependent_materialized_views = 1")
	if err != nil {
		return err
	}

	// Если Send() не отработает успешно, Abort() отменит ожидание ClickHouse.
	defer func() {
		_ = batch.Abort()
	}()

	appendedCount := 0

	for i := 0; i < len(batchData); i++ {
		var event Event
		err := json.Unmarshal(batchData[i], &event)
		if err != nil {
			log.Println(fmt.Errorf("ошибка расшифровки event: %w", err))
			continue
		}

		err = batch.Append(
			event.EventId,
			event.EventType,
			event.UserID,
			event.SessionID,
			event.TargetID,
			string(event.MetaData),
			event.ClientTime,
			event.OS,
			event.Browser,
			event.Device,
			xxhash.Sum64String(event.VisitorID),
			event.CountryIsoCode,
		)

		if err != nil {
			return err
		}
		appendedCount++
	}

	if appendedCount == 0 {
		return nil
	}

	return batch.Send()
}
