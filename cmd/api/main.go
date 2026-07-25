package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/DarkTheme404/real-time-analytics-platform/internal/api"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/cache"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/config"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/kafka"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/storage"
)

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

	handler := api.NewHandler(storage, redisCache, producer, logger)
	router := handler.SetupRouter()

	metricsServer := handler.StartMetricsServer(cfg.Metrics.Port)

	apiServer := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.API.Port),
		Handler:      router,
		ReadTimeout:  cfg.API.ReadTimeout,
		WriteTimeout: cfg.API.WriteTimeout,
	}

	go func() {
		logger.Info("Starting API server", zap.String("port", cfg.API.Port))
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("API server failed", zap.Error(err))
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Received shutdown signal, starting graceful shutdown...")

	if err := metricsServer.Close(); err != nil {
		logger.Error("Failed to close metrics server", zap.Error(err))
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("API server shutdown failed", zap.Error(err))
	}

	cancel()

	logger.Info("API server stopped gracefully")
}

func initLogger(level string) *zap.Logger {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(lvl),
		Development:      false,
		Encoding:         "json",
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	return logger
}
