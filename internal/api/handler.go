package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/DarkTheme404/real-time-analytics-platform/internal/cache"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/kafka"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/metrics"
	"github.com/DarkTheme404/real-time-analytics-platform/internal/storage"
)

type Handler struct {
	storage  *storage.ClickHouseStorage
	cache    *cache.RedisCache
	producer *kafka.Producer
	logger   *zap.Logger
}

func NewHandler(storage *storage.ClickHouseStorage, cache *cache.RedisCache, producer *kafka.Producer, logger *zap.Logger) *Handler {
	return &Handler{
		storage:  storage,
		cache:    cache,
		producer: producer,
		logger:   logger,
	}
}

func (h *Handler) SetupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(h.loggingMiddleware())
	router.Use(h.metricsMiddleware())

	router.GET("/health", h.HealthCheck)
	router.GET("/ready", h.ReadinessCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	api := router.Group("/api/v1")
	{
		api.POST("/events", h.IngestEvent)
		api.POST("/events/batch", h.IngestEventBatch)
		api.GET("/events", h.QueryEvents)
		api.GET("/analytics/aggregations", h.QueryAggregations)
		api.GET("/analytics/summary", h.GetAnalyticsSummary)
	}

	return router
}

func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "analytics-api",
		"time":    time.Now().UTC(),
	})
}

func (h *Handler) ReadinessCheck(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	checks := map[string]string{}

	if err := h.storage.Ping(ctx); err != nil {
		checks["clickhouse"] = "unhealthy"
	} else {
		checks["clickhouse"] = "healthy"
	}

	if err := h.cache.Ping(ctx); err != nil {
		checks["redis"] = "unhealthy"
	} else {
		checks["redis"] = "healthy"
	}

	allHealthy := true
	for _, status := range checks {
		if status != "healthy" {
			allHealthy = false
			break
		}
	}

	statusCode := http.StatusOK
	if !allHealthy {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, gin.H{
		"status": map[string]interface{}{
			"checks":  checks,
			"healthy": allHealthy,
		},
		"service": "analytics-api",
		"time":    time.Now().UTC(),
	})
}

func (h *Handler) IngestEvent(c *gin.Context) {
	var event storage.Event
	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request: %v", err)})
		return
	}

	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	if err := h.storage.BatchInsert(c.Request.Context(), []storage.Event{event}); err != nil {
		h.logger.Error("Failed to ingest event", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest event"})
		return
	}

	metrics.EventsIngestedTotal.WithLabelValues(event.Source, event.EventType).Inc()

	c.JSON(http.StatusCreated, gin.H{
		"id":      event.ID,
		"status":  "accepted",
		"message": "event ingested successfully",
	})
}

func (h *Handler) IngestEventBatch(c *gin.Context) {
	var events []storage.Event
	if err := c.ShouldBindJSON(&events); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request: %v", err)})
		return
	}

	for i := range events {
		if events[i].ID == "" {
			events[i].ID = uuid.New().String()
		}
		if events[i].Timestamp.IsZero() {
			events[i].Timestamp = time.Now().UTC()
		}
	}

	if err := h.storage.BatchInsert(c.Request.Context(), events); err != nil {
		h.logger.Error("Failed to ingest event batch", zap.Error(err), zap.Int("count", len(events)))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest events"})
		return
	}

	for _, event := range events {
		metrics.EventsIngestedTotal.WithLabelValues(event.Source, event.EventType).Inc()
	}

	c.JSON(http.StatusCreated, gin.H{
		"count":   len(events),
		"status":  "accepted",
		"message": "events ingested successfully",
	})
}

func (h *Handler) QueryEvents(c *gin.Context) {
	userID := c.Query("user_id")
	eventType := c.Query("event_type")
	granularity := c.Query("granularity")

	fromStr := c.DefaultQuery("from", time.Now().Add(-24*time.Hour).Format(time.RFC3339))
	toStr := c.DefaultQuery("to", time.Now().Format(time.RFC3339))
	limit := 100

	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid 'from' parameter"})
		return
	}

	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid 'to' parameter"})
		return
	}

	cacheKey := fmt.Sprintf("events:%s:%s:%s:%s:%d:%d", userID, eventType, granularity, fromStr, to.Unix(), limit)

	var cached []storage.Event
	if err := h.cache.Get(c.Request.Context(), cacheKey, &cached); err == nil && cached != nil {
		metrics.CacheHitsTotal.WithLabelValues("events").Inc()
		c.JSON(http.StatusOK, gin.H{"data": cached, "cached": true})
		return
	}
	metrics.CacheMissesTotal.WithLabelValues("events").Inc()

	events, err := h.storage.QueryEvents(c.Request.Context(), userID, eventType, from, to, limit)
	if err != nil {
		h.logger.Error("Failed to query events", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query events"})
		return
	}

	_ = h.cache.Set(c.Request.Context(), cacheKey, events, 2*time.Minute)

	c.JSON(http.StatusOK, gin.H{"data": events, "count": len(events)})
}

func (h *Handler) QueryAggregations(c *gin.Context) {
	eventType := c.Query("event_type")
	granularity := c.DefaultQuery("granularity", "hour")

	fromStr := c.DefaultQuery("from", time.Now().Add(-24*time.Hour).Format(time.RFC3339))
	toStr := c.DefaultQuery("to", time.Now().Format(time.RFC3339))

	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid 'from' parameter"})
		return
	}

	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid 'to' parameter"})
		return
	}

	cacheKey := fmt.Sprintf("aggregations:%s:%s:%s:%s:%d", eventType, granularity, fromStr, toStr, 0)

	var cached []map[string]interface{}
	if err := h.cache.Get(c.Request.Context(), cacheKey, &cached); err == nil && cached != nil {
		metrics.CacheHitsTotal.WithLabelValues("aggregations").Inc()
		c.JSON(http.StatusOK, gin.H{"data": cached, "cached": true})
		return
	}
	metrics.CacheMissesTotal.WithLabelValues("aggregations").Inc()

	timer := metrics.QueryDurationSeconds.WithLabelValues("aggregations").NewTimer()
	defer timer.ObserveDuration()

	results, err := h.storage.QueryAggregations(c.Request.Context(), eventType, from, to, granularity)
	if err != nil {
		h.logger.Error("Failed to query aggregations", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query aggregations"})
		return
	}

	_ = h.cache.Set(c.Request.Context(), cacheKey, results, 2*time.Minute)

	c.JSON(http.StatusOK, gin.H{"data": results, "count": len(results)})
}

func (h *Handler) GetAnalyticsSummary(c *gin.Context) {
	eventType := c.Query("event_type")
	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()

	timer := metrics.QueryDurationSeconds.WithLabelValues("summary").NewTimer()
	defer timer.ObserveDuration()

	results, err := h.storage.QueryAggregations(c.Request.Context(), eventType, from, to, "hour")
	if err != nil {
		h.logger.Error("Failed to get analytics summary", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get analytics summary"})
		return
	}

	var totalEvents uint64
	var totalUniqueUsers uint64
	for _, r := range results {
		if te, ok := r["total_events"].(uint64); ok {
			totalEvents += te
		}
		if uu, ok := r["unique_users"].(uint64); ok {
			totalUniqueUsers += uu
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"summary": gin.H{
			"total_events":     totalEvents,
			"total_users":      totalUniqueUsers,
			"period_hours":     24,
			"event_type":       eventType,
			"data_points":      len(results),
		},
		"time_range": gin.H{
			"from": from,
			"to":   to,
		},
	})
}

func (h *Handler) loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		h.logger.Info("Request",
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", status),
			zap.Duration("latency", latency),
			zap.String("client_ip", c.ClientIP()),
			zap.Int("body_size", c.Writer.Size()),
		)
	}
}

func (h *Handler) metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		latency := time.Since(start).Seconds()
		status := c.Writer.Status()
		path := c.Request.URL.Path

		metrics.QueryDurationSeconds.WithLabelValues(fmt.Sprintf("%s_%s", c.Request.Method, path)).Observe(latency)
		_ = status

		metrics.EventsProcessedTotal.WithLabelValues("success").Inc()
	}
}

func (h *Handler) StartMetricsServer(port string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: mux,
	}

	go func() {
		h.logger.Info("Starting metrics server", zap.String("port", port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			h.logger.Error("Metrics server failed", zap.Error(err))
		}
	}()

	return server
}
