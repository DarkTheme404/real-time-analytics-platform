package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/DarkTheme404/real-time-analytics-platform/internal/cache"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/config"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/kafka"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/metrics"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/storage"
)

type AggregationEvent struct {
	EventType   string                 `json:"event_type"`
	UserID      string                 `json:"user_id"`
	Source      string                 `json:"source"`
	Data        map[string]interface{} `json:"data"`
	Timestamp   time.Time              `json:"timestamp"`
	Aggregation map[string]interface{} `json:"aggregation"`
}

type AggregationWorker struct {
	storage       *storage.ClickHouseStorage
	cache         *cache.RedisCache
	producer      *kafka.Producer
	logger        *zap.Logger
	batchSize     int
	flushInterval time.Duration
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger := initLogger(cfg.Log.Level)
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage, err := storage.NewClickHouseStorage(cfg.ClickHouse, logger)
	if err != nil {
		logger.Fatal("Failed to connect to ClickHouse", zap.Error(err))
	}
	defer storage.Close()

	redisCache, err := cache.NewRedisCache(cfg.Redis, logger)
	if err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
	}
	defer redisCache.Close()

	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.DLQTopic, logger)
	if err != nil {
		logger.Fatal("Failed to create Kafka producer", zap.Error(err))
	}
	defer producer.Close()

	worker := &AggregationWorker{
		storage:       storage,
		cache:         redisCache,
		producer:      producer,
		logger:        logger,
		batchSize:     cfg.Worker.BatchSize,
		flushInterval: cfg.Worker.FlushInterval,
	}

	consumer, err := kafka.NewConsumer(cfg.Kafka, worker, logger)
	if err != nil {
		logger.Fatal("Failed to create Kafka consumer", zap.Error(err))
	}

	metrics.WorkersActive.Inc()
	defer metrics.WorkersActive.Dec()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		consumer.Start(ctx)
		return nil
	})

	g.Go(func() error {
		worker.runAggregationLoop(ctx)
		return nil
	})

	g.Go(func() error {
		<-ctx.Done()
		logger.Info("Shutting down worker...")
		return consumer.Close()
	})

	g.Go(func() error {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-sigChan:
			logger.Info("Received shutdown signal")
			cancel()
		case <-ctx.Done():
		}
		return nil
	})

	logger.Info("Worker started", zap.String("service", "analytics-worker"))

	if err := g.Wait(); err != nil {
		logger.Error("Worker stopped with error", zap.Error(err))
	}

	logger.Info("Worker stopped gracefully")
}

func (aw *AggregationWorker) ProcessMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var event AggregationEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		aw.logger.Error("Failed to unmarshal aggregation event", zap.Error(err))
		return aw.producer.SendToDLQ(msg.Key, msg.Value, fmt.Sprintf("unmarshal error: %v", err))
	}

	metrics.EventsProcessedTotal.WithLabelValues("aggregation").Inc()

	aggregation := aw.computeAggregation(event)

	aggregatedEvent := AggregationEvent{
		EventType:   event.EventType,
		UserID:      event.UserID,
		Source:      event.Source,
		Data:        event.Data,
		Timestamp:   event.Timestamp,
		Aggregation: aggregation,
	}

	value, err := json.Marshal(aggregatedEvent)
	if err != nil {
		aw.logger.Error("Failed to marshal aggregated event", zap.Error(err))
		return aw.producer.SendToDLQ(msg.Key, msg.Value, fmt.Sprintf("marshal error: %v", err))
	}

	if err := aw.producer.SendMessage(msg.Key, value, map[string]string{
		"event_type": event.EventType,
		"source":     event.Source,
		"aggregated": "true",
	}); err != nil {
		aw.logger.Error("Failed to send aggregated event", zap.Error(err))
		return err
	}

	aw.logger.Debug("Event aggregated",
		zap.String("event_type", event.EventType),
		zap.String("user_id", event.UserID),
	)

	return nil
}

// computeAggregation - бизнес-логика обогащения события.
// Категоризирует страницы и бакетизирует суммы для дальнейшей аналитики.
func (aw *AggregationWorker) computeAggregation(event AggregationEvent) map[string]interface{} {
	aggregation := map[string]interface{}{
		"processed_at": time.Now().UTC(),
		"data_size":    len(event.Data),
	}

	if event.Data != nil {
		if page, ok := event.Data["page"].(string); ok {
			aggregation["page_category"] = categorizePage(page)
		}
		if amount, ok := event.Data["amount"].(float64); ok {
			aggregation["amount_bucket"] = bucketAmount(amount)
		}
	}

	return aggregation
}

func categorizePage(page string) string {
	categories := map[string]string{
		"/home":     "homepage",
		"/products": "catalog",
		"/cart":     "checkout",
		"/checkout": "checkout",
		"/profile":  "account",
	}

	for pattern, category := range categories {
		if len(page) >= len(pattern) && page[:len(pattern)] == pattern {
			return category
		}
	}
	return "other"
}

func bucketAmount(amount float64) string {
	switch {
	case amount < 10:
		return "small"
	case amount < 50:
		return "medium"
	case amount < 100:
		return "large"
	default:
		return "premium"
	}
}

// runAggregationLoop - периодический flush кэша агрегаций.
// Каждые flushInterval секунд забирает накопленные агрегации из Redis и чистит ключи.
func (aw *AggregationWorker) runAggregationLoop(ctx context.Context) {
	ticker := time.NewTicker(aw.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			aw.flushAggregations(ctx)
		}
	}
}

func (aw *AggregationWorker) flushAggregations(ctx context.Context) {
	aw.logger.Debug("Flushing aggregation cache")

	keys, err := aw.cache.Keys(ctx, "aggregation:*")
	if err != nil {
		aw.logger.Error("Failed to get aggregation keys", zap.Error(err))
		return
	}

	for _, key := range keys {
		var aggregation map[string]interface{}
		if err := aw.cache.Get(ctx, key, &aggregation); err != nil {
			continue
		}

		aw.logger.Debug("Flushed aggregation", zap.String("key", key))
	}

	if err := aw.cache.Delete(ctx, keys...); err != nil {
		aw.logger.Error("Failed to delete aggregation keys", zap.Error(err))
	}
}

func initLogger(level string) *zap.Logger {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}

	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(lvl),
		Development:      false,
		Encoding:         "json",
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := cfg.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	return logger
}
