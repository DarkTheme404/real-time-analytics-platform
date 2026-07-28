package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/DarkTheme404/real-time-analytics-platform/internal/config"
)

type RedisCache struct {
	client *redis.Client
	logger *zap.Logger
	ttl    time.Duration
}

func NewRedisCache(cfg config.RedisConfig, logger *zap.Logger) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MinIdleConns: 3,
		PoolTimeout:  30 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	logger.Info("Connected to Redis successfully", zap.String("addr", cfg.Addr))

	return &RedisCache{
		client: client,
		logger: logger,
		ttl:    cfg.CacheTTL,
	}, nil
}

func (c *RedisCache) Get(ctx context.Context, key string, dest interface{}) error {
	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if err := json.Unmarshal([]byte(val), dest); err != nil {
		return fmt.Errorf("failed to unmarshal cache value: %w", err)
	}

	return nil
}

func (c *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl ...time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	cacheTTL := c.ttl
	if len(ttl) > 0 {
		cacheTTL = ttl[0]
	}

	if err := c.client.Set(ctx, key, data, cacheTTL).Err(); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	c.logger.Debug("Cache set", zap.String("key", key), zap.Duration("ttl", cacheTTL))
	return nil
}

func (c *RedisCache) Delete(ctx context.Context, keys ...string) error {
	if err := c.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("failed to delete keys: %w", err)
	}

	c.logger.Debug("Cache deleted", zap.Strings("keys", keys))
	return nil
}

func (c *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	val, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check key existence: %w", err)
	}
	return val > 0, nil
}

func (c *RedisCache) SetNX(ctx context.Context, key string, value interface{}, ttl ...time.Duration) (bool, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("failed to marshal value: %w", err)
	}

	cacheTTL := c.ttl
	if len(ttl) > 0 {
		cacheTTL = ttl[0]
	}

	ok, err := c.client.SetNX(ctx, key, data, cacheTTL).Result()
	if err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return ok, nil
}

func (c *RedisCache) Incr(ctx context.Context, key string) (int64, error) {
	val, err := c.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to increment key %s: %w", key, err)
	}
	return val, nil
}

func (c *RedisCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if err := c.client.Expire(ctx, key, ttl).Err(); err != nil {
		return fmt.Errorf("failed to set expiry for key %s: %w", key, err)
	}
	return nil
}

func (c *RedisCache) Close() error {
	c.logger.Info("Closing Redis connection")
	return c.client.Close()
}

func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *RedisCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get keys with pattern %s: %w", pattern, err)
	}
	return keys, nil
}

func (c *RedisCache) FlushAll(ctx context.Context) error {
	if err := c.client.FlushAll(ctx).Err(); err != nil {
		return fmt.Errorf("failed to flush all keys: %w", err)
	}
	return nil
}
