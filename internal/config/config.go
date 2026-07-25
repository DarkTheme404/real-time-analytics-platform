package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Kafka    KafkaConfig
	ClickHouse ClickHouseConfig
	Redis    RedisConfig
	API      APIConfig
	Worker   WorkerConfig
	Metrics  MetricsConfig
	Log      LogConfig
}

type KafkaConfig struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	DLQTopic      string
}

type ClickHouseConfig struct {
	DSN             string
	BatchSize       int
	FlushInterval   time.Duration
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	CacheTTL time.Duration
}

type APIConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type WorkerConfig struct {
	BatchSize     int
	FlushInterval time.Duration
}

type MetricsConfig struct {
	Port string
}

type LogConfig struct {
	Level string
}

func Load() (*Config, error) {
	cfg := &Config{}

	cfg.Kafka = KafkaConfig{
		Brokers:       getEnvSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		Topic:         getEnv("KAFKA_TOPIC", "analytics-events"),
		ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "analytics-platform"),
		DLQTopic:      getEnv("KAFKA_DLQ_TOPIC", "analytics-events-dlq"),
	}

	dsn := getEnv("CLICKHOUSE_DSN", "tcp://localhost:9000/analytics?username=default&password=")
	batchSize := getEnvInt("WORKER_BATCH_SIZE", 1000)
	flushInterval := getEnvDuration("WORKER_FLUSH_INTERVAL", 5*time.Second)

	cfg.ClickHouse = ClickHouseConfig{
		DSN:             dsn,
		BatchSize:       batchSize,
		FlushInterval:   flushInterval,
		MaxOpenConns:    getEnvInt("CLICKHOUSE_MAX_OPEN_CONNS", 10),
		MaxIdleConns:    getEnvInt("CLICKHOUSE_MAX_IDLE_CONNS", 5),
		ConnMaxLifetime: getEnvDuration("CLICKHOUSE_CONN_MAX_LIFETIME", 5*time.Minute),
	}

	cfg.Redis = RedisConfig{
		Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       getEnvInt("REDIS_DB", 0),
		CacheTTL: getEnvDuration("REDIS_CACHE_TTL", 5*time.Minute),
	}

	cfg.API = APIConfig{
		Port:         getEnv("API_PORT", "8080"),
		ReadTimeout:  getEnvDuration("API_READ_TIMEOUT", 30*time.Second),
		WriteTimeout: getEnvDuration("API_WRITE_TIMEOUT", 30*time.Second),
	}

	cfg.Worker = WorkerConfig{
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
	}

	cfg.Metrics = MetricsConfig{
		Port: getEnv("METRICS_PORT", "9090"),
	}

	cfg.Log = LogConfig{
		Level: getEnv("LOG_LEVEL", "info"),
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("at least one Kafka broker is required")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("Kafka topic is required")
	}
	if c.ClickHouse.DSN == "" {
		return fmt.Errorf("ClickHouse DSN is required")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("Redis address is required")
	}
	if c.API.Port == "" {
		return fmt.Errorf("API port is required")
	}
	return nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if dur, err := time.ParseDuration(val); err == nil {
			return dur
		}
	}
	return defaultVal
}

func getEnvSlice(key string, defaultVal []string) []string {
	if val := os.Getenv(key); val != "" {
		parts := strings.Split(val, ",")
		result := make([]string, 0, len(parts))
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				result = append(result, trimmed)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultVal
}
