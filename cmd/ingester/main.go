package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/DarkTheme404/real-time-analytics-platform/internal/config"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/kafka"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/metrics"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/storage"
)

type EventProcessor struct {
	storage  *storage.ClickHouseStorage
	producer *kafka.Producer
	logger   *zap.Logger
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

	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.DLQTopic, logger)
	if err != nil {
		logger.Fatal("Failed to create Kafka producer", zap.Error(err))
	}
	defer producer.Close()

	processor := &EventProcessor{
		storage:  storage,
		producer: producer,
		logger:   logger,
	}

	consumer, err := kafka.NewConsumer(cfg.Kafka, processor, logger)
	if err != nil {
		logger.Fatal("Failed to create Kafka consumer", zap.Error(err))
	}

	metricsServer := startMetricsServer(cfg.Metrics.Port, logger)
	defer metricsServer.Close()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		consumer.Start(ctx)
		return nil
	})

	g.Go(func() error {
		<-ctx.Done()
		logger.Info("Shutting down consumer...")
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

	logger.Info("Ingester started", zap.String("service", "analytics-ingester"))

	if err := g.Wait(); err != nil {
		logger.Error("Ingester stopped with error", zap.Error(err))
	}

	logger.Info("Ingester stopped gracefully")
}

func (ep *EventProcessor) ProcessMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var event storage.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		ep.logger.Error("Failed to unmarshal event", zap.Error(err))
		return ep.producer.SendToDLQ(msg.Key, msg.Value, fmt.Sprintf("unmarshal error: %v", err))
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	metrics.EventsIngestedTotal.WithLabelValues(event.Source, event.EventType).Inc()

	if err := ep.storage.BatchInsert(ctx, []storage.Event{event}); err != nil {
		ep.logger.Error("Failed to insert event", zap.Error(err), zap.String("event_id", event.ID))
		return ep.producer.SendToDLQ(msg.Key, msg.Value, fmt.Sprintf("insert error: %v", err))
	}

	ep.logger.Debug("Event processed",
		zap.String("event_id", event.ID),
		zap.String("event_type", event.EventType),
		zap.String("user_id", event.UserID),
	)

	metrics.EventsProcessedTotal.WithLabelValues("success").Inc()
	return nil
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

func startMetricsServer(port string, logger *zap.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"healthy","service":"analytics-ingester"}`)
	})
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("Starting metrics server", zap.String("port", port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server failed", zap.Error(err))
		}
	}()

	return server
}
