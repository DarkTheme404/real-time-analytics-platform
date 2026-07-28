package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"github.com/DarkTheme404/real-time-analytics-platform/internal/config"
)

type Event struct {
	ID        string
	EventType string
	UserID    string
	Source    string
	Data      map[string]interface{}
	Timestamp time.Time
}

type ClickHouseStorage struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewClickHouseStorage(cfg config.ClickHouseConfig, logger *zap.Logger) (*ClickHouseStorage, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.DSN},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	storage := &ClickHouseStorage{
		db:     conn,
		logger: logger,
	}

	if err := storage.migrate(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	logger.Info("Connected to ClickHouse successfully")
	return storage, nil
}

func (s *ClickHouseStorage) migrate() error {
	migrations := []string{
		// Основная таблица событий. TTL 90 дней - старые данные автоматически удаляются.
		// Партиционирование по месяцам для быстрых запросов за период.
		`CREATE TABLE IF NOT EXISTS analytics_events (
			id String,
			event_type String,
			user_id String,
			source String,
			data String,
			timestamp DateTime64(3),
			ingested_at DateTime64(3) DEFAULT now64(3)
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (event_type, user_id, timestamp)
		TTL timestamp + INTERVAL 90 DAY`,

		// Aggregations table - заполняется через materialized view
		`CREATE TABLE IF NOT EXISTS analytics_aggregations (
			event_type String,
			source String,
			time_bucket DateTime,
			total_events UInt64,
			unique_users UInt64,
			avg_data_size Float64,
			updated_at DateTime64(3)
		) ENGINE = SummingMergeTree()
		PARTITION BY toYYYYMM(time_bucket)
		ORDER BY (event_type, source, time_bucket)`,

		// MV автоматически агрегирует события по часам при вставке
		`CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_events_mv
		TO analytics_aggregations
		AS SELECT
			event_type,
			source,
			toStartOfHour(timestamp) AS time_bucket,
			toUInt64(1) AS total_events,
			toUInt64(1) AS unique_users,
			toFloat64(length(data)) AS avg_data_size,
			now64(3) AS updated_at
		FROM analytics_events
		GROUP BY event_type, source, time_bucket`,

		// Таблицы для агрегированных статистик с use AggregateFunction для精确 uniq
		`CREATE TABLE IF NOT EXISTS analytics_hourly_stats (
			hour DateTime,
			event_type String,
			total_events UInt64,
			unique_users AggregateFunction(uniq, String),
			event_sizes AggregateFunction(avg, UInt64)
		) ENGINE = AggregatingMergeTree()
		PARTITION BY toYYYYMM(hour)
		ORDER BY (event_type, hour)`,

		`CREATE TABLE IF NOT EXISTS analytics_daily_stats (
			day Date,
			event_type String,
			total_events UInt64,
			unique_users AggregateFunction(uniq, String),
			event_sizes AggregateFunction(avg, UInt64)
		) ENGINE = AggregatingMergeTree()
		PARTITION BY toYYYYMM(day)
		ORDER BY (event_type, day)`,
	}

	for _, migration := range migrations {
		if err := s.db.Exec(context.Background(), migration); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	s.logger.Info("ClickHouse migrations completed")
	return nil
}

func (s *ClickHouseStorage) BatchInsert(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := s.db.PrepareBatch(ctx, "INSERT INTO analytics_events")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, event := range events {
		dataJSON, err := json.Marshal(event.Data)
		if err != nil {
			s.logger.Warn("Failed to marshal event data", zap.Error(err))
			dataJSON = []byte("{}")
		}

		err = batch.Append(
			event.ID,
			event.EventType,
			event.UserID,
			event.Source,
			string(dataJSON),
			event.Timestamp,
		)
		if err != nil {
			s.logger.Error("Failed to append event to batch", zap.Error(err))
			continue
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	s.logger.Debug("Batch inserted events", zap.Int("count", len(events)))
	return nil
}

func (s *ClickHouseStorage) QueryEvents(ctx context.Context, userID, eventType string, from, to time.Time, limit int) ([]Event, error) {
	query := `
		SELECT id, event_type, user_id, source, data, timestamp
		FROM analytics_events
		WHERE 1=1`

	var args []interface{}

	if userID != "" {
		query += " AND user_id = ?"
		args = append(args, userID)
	}
	if eventType != "" {
		query += " AND event_type = ?"
		args = append(args, eventType)
	}
	if !from.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, from)
	}
	if !to.IsZero() {
		query += " AND timestamp <= ?"
		args = append(args, to)
	}

	query += " ORDER BY timestamp DESC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var event Event
		var dataStr string
		if err := rows.Scan(
			&event.ID,
			&event.EventType,
			&event.UserID,
			&event.Source,
			&dataStr,
			&event.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}

		event.Data = make(map[string]interface{})
		if err := json.Unmarshal([]byte(dataStr), &event.Data); err != nil {
			s.logger.Warn("Failed to unmarshal event data", zap.Error(err))
		}

		events = append(events, event)
	}

	return events, rows.Err()
}

// QueryAggregations - динамическая агрегация по time bucket.
// granularity определяет размер окна: minute/hour/day.
func (s *ClickHouseStorage) QueryAggregations(ctx context.Context, eventType string, from, to time.Time, granularity string) ([]map[string]interface{}, error) {
	var timeFunc string
	switch granularity {
	case "hour":
		timeFunc = "toStartOfHour(timestamp)"
	case "day":
		timeFunc = "toStartOfDay(timestamp)"
	case "minute":
		timeFunc = "toStartOfMinute(timestamp)"
	default:
		timeFunc = "toStartOfHour(timestamp)"
	}

	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			event_type,
			count() as total_events,
			uniqExact(user_id) as unique_users,
			avg(length(data)) as avg_data_size
		FROM analytics_events
		WHERE 1=1`, timeFunc)

	var args []interface{}

	if eventType != "" {
		query += " AND event_type = ?"
		args = append(args, eventType)
	}
	if !from.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, from)
	}
	if !to.IsZero() {
		query += " AND timestamp <= ?"
		args = append(args, to)
	}

	query += " GROUP BY time_bucket, event_type ORDER BY time_bucket"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query aggregations: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var (
			timeBucket  time.Time
			evtType     string
			totalEvents uint64
			uniqueUsers uint64
			avgDataSize float64
		)
		if err := rows.Scan(&timeBucket, &evtType, &totalEvents, &uniqueUsers, &avgDataSize); err != nil {
			return nil, fmt.Errorf("failed to scan aggregation: %w", err)
		}
		results = append(results, map[string]interface{}{
			"time_bucket":   timeBucket,
			"event_type":    evtType,
			"total_events":  totalEvents,
			"unique_users":  uniqueUsers,
			"avg_data_size": avgDataSize,
		})
	}

	return results, rows.Err()
}

func (s *ClickHouseStorage) Close() error {
	s.logger.Info("Closing ClickHouse connection")
	return s.db.Close()
}

func (s *ClickHouseStorage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}
