package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewClickHouseStorage(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	cfg := ClickHouseConfig{
		DSN:             "tcp://localhost:9000/analytics?username=default&password=",
		BatchSize:       1000,
		FlushInterval:   5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	storage, err := NewClickHouseStorage(cfg, logger)
	if err != nil {
		t.Skip("ClickHouse not available for integration test")
	}
	defer storage.Close()

	assert.NotNil(t, storage)
}

func TestClickHouseStorage_BatchInsert(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	cfg := ClickHouseConfig{
		DSN:             "tcp://localhost:9000/analytics?username=default&password=",
		BatchSize:       1000,
		FlushInterval:   5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	storage, err := NewClickHouseStorage(cfg, logger)
	if err != nil {
		t.Skip("ClickHouse not available for integration test")
	}
	defer storage.Close()

	events := []Event{
		{
			ID:        "test-1",
			EventType: "page_view",
			UserID:    "user-123",
			Source:    "web",
			Data:      map[string]interface{}{"page": "/home"},
			Timestamp: time.Now().UTC(),
		},
		{
			ID:        "test-2",
			EventType: "click",
			UserID:    "user-456",
			Source:    "mobile",
			Data:      map[string]interface{}{"button": "buy"},
			Timestamp: time.Now().UTC(),
		},
	}

	err = storage.BatchInsert(context.Background(), events)
	assert.NoError(t, err)
}

func TestClickHouseStorage_BatchInsertEmpty(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	cfg := ClickHouseConfig{
		DSN:             "tcp://localhost:9000/analytics?username=default&password=",
		BatchSize:       1000,
		FlushInterval:   5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	storage, err := NewClickHouseStorage(cfg, logger)
	if err != nil {
		t.Skip("ClickHouse not available for integration test")
	}
	defer storage.Close()

	err = storage.BatchInsert(context.Background(), []Event{})
	assert.NoError(t, err)
}

func TestClickHouseStorage_QueryEvents(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	cfg := ClickHouseConfig{
		DSN:             "tcp://localhost:9000/analytics?username=default&password=",
		BatchSize:       1000,
		FlushInterval:   5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	storage, err := NewClickHouseStorage(cfg, logger)
	if err != nil {
		t.Skip("ClickHouse not available for integration test")
	}
	defer storage.Close()

	events, err := storage.QueryEvents(context.Background(), "", "", time.Now().Add(-1*time.Hour), time.Now(), 10)
	assert.NoError(t, err)
	assert.NotNil(t, events)
}

func TestClickHouseStorage_QueryAggregations(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	cfg := ClickHouseConfig{
		DSN:             "tcp://localhost:9000/analytics?username=default&password=",
		BatchSize:       1000,
		FlushInterval:   5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	storage, err := NewClickHouseStorage(cfg, logger)
	if err != nil {
		t.Skip("ClickHouse not available for integration test")
	}
	defer storage.Close()

	results, err := storage.QueryAggregations(context.Background(), "", time.Now().Add(-24*time.Hour), time.Now(), "hour")
	assert.NoError(t, err)
	assert.NotNil(t, results)
}

func TestClickHouseStorage_Ping(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	cfg := ClickHouseConfig{
		DSN:             "tcp://localhost:9000/analytics?username=default&password=",
		BatchSize:       1000,
		FlushInterval:   5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	storage, err := NewClickHouseStorage(cfg, logger)
	if err != nil {
		t.Skip("ClickHouse not available for integration test")
	}
	defer storage.Close()

	err = storage.Ping(context.Background())
	assert.NoError(t, err)
}

func TestClickHouseStorage_Close(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	cfg := ClickHouseConfig{
		DSN:             "tcp://localhost:9000/analytics?username=default&password=",
		BatchSize:       1000,
		FlushInterval:   5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	storage, err := NewClickHouseStorage(cfg, logger)
	if err != nil {
		t.Skip("ClickHouse not available for integration test")
	}

	err = storage.Close()
	assert.NoError(t, err)
}

func TestEvent_Fields(t *testing.T) {
	event := Event{
		ID:        "test-id",
		EventType: "page_view",
		UserID:    "user-123",
		Source:    "web",
		Data:      map[string]interface{}{"key": "value"},
		Timestamp: time.Now().UTC(),
	}

	assert.Equal(t, "test-id", event.ID)
	assert.Equal(t, "page_view", event.EventType)
	assert.Equal(t, "user-123", event.UserID)
	assert.Equal(t, "web", event.Source)
	assert.Equal(t, "value", event.Data["key"])
}

func TestClickHouseConfig_Defaults(t *testing.T) {
	cfg := ClickHouseConfig{
		DSN:             "tcp://localhost:9000/analytics",
		BatchSize:       1000,
		FlushInterval:   5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	require.NotEmpty(t, cfg.DSN)
	require.Equal(t, 1000, cfg.BatchSize)
	require.Equal(t, 5*time.Second, cfg.FlushInterval)
	require.Equal(t, 10, cfg.MaxOpenConns)
	require.Equal(t, 5, cfg.MaxIdleConns)
	require.Equal(t, 5*time.Minute, cfg.ConnMaxLifetime)
}
