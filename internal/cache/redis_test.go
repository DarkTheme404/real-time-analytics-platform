package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestCache(t *testing.T) *RedisCache {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	cfg := RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		CacheTTL: 5 * time.Minute,
	}
	cache, err := NewRedisCache(cfg, logger)
	if err != nil {
		t.Skip("Redis not available for integration test")
	}
	t.Cleanup(func() { cache.Close() })
	return cache
}

func TestNewRedisCache(t *testing.T) {
	cache := newTestCache(t)
	assert.NotNil(t, cache)
}

func TestRedisCache_SetAndGet(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	key := "test-key"
	value := map[string]string{"name": "test", "value": "123"}

	err := cache.Set(ctx, key, value)
	require.NoError(t, err)

	var result map[string]string
	err = cache.Get(ctx, key, &result)
	require.NoError(t, err)
	assert.Equal(t, value, result)
}

func TestRedisCache_GetNonExistent(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	var result map[string]string
	err := cache.Get(ctx, "non-existent-key", &result)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestRedisCache_Delete(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	key := "delete-test"
	err := cache.Set(ctx, key, "value")
	require.NoError(t, err)

	err = cache.Delete(ctx, key)
	assert.NoError(t, err)

	exists, err := cache.Exists(ctx, key)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestRedisCache_Exists(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	key := "exists-test"

	exists, err := cache.Exists(ctx, key)
	require.NoError(t, err)
	assert.False(t, exists)

	err = cache.Set(ctx, key, "value")
	require.NoError(t, err)

	exists, err = cache.Exists(ctx, key)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestRedisCache_SetNX(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	key := "setnx-test"

	ok, err := cache.SetNX(ctx, key, "value1")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = cache.SetNX(ctx, key, "value2")
	require.NoError(t, err)
	assert.False(t, ok)

	var result string
	err = cache.Get(ctx, key, &result)
	require.NoError(t, err)
	assert.Equal(t, "value1", result)
}

func TestRedisCache_Incr(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	key := "incr-test"

	val, err := cache.Incr(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)

	val, err = cache.Incr(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)
}

func TestRedisCache_Ping(t *testing.T) {
	cache := newTestCache(t)
	err := cache.Ping(context.Background())
	assert.NoError(t, err)
}

func TestRedisCache_Close(t *testing.T) {
	cache := newTestCache(t)
	err := cache.Close()
	assert.NoError(t, err)
}

func TestRedisCache_FlushAll(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	_ = cache.Set(ctx, "flush-test", "value")

	err := cache.FlushAll(ctx)
	assert.NoError(t, err)
}

func TestRedisConfig_Defaults(t *testing.T) {
	cfg := RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		CacheTTL: 5 * time.Minute,
	}

	require.Equal(t, "localhost:6379", cfg.Addr)
	require.Empty(t, cfg.Password)
	require.Equal(t, 0, cfg.DB)
	require.Equal(t, 5*time.Minute, cfg.CacheTTL)
}
