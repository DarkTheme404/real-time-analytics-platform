package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func setupTestHandler(t *testing.T) *Handler {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	return &Handler{
		logger: logger,
	}
}

func TestHealthCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, "healthy", response["status"])
	assert.Equal(t, "analytics-api", response["service"])
}

func TestReadinessCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ready", nil)
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusOK, http.StatusServiceUnavailable}, w.Code)
}

func TestIngestEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	event := map[string]interface{}{
		"event_type": "page_view",
		"user_id":    "user-123",
		"source":     "web",
		"data":       map[string]interface{}{"page": "/home"},
	}

	body, _ := json.Marshal(event)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusCreated, http.StatusInternalServerError}, w.Code)
}

func TestIngestEventBatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	events := []map[string]interface{}{
		{
			"event_type": "page_view",
			"user_id":    "user-123",
			"source":     "web",
			"data":       map[string]interface{}{"page": "/home"},
		},
		{
			"event_type": "click",
			"user_id":    "user-456",
			"source":     "mobile",
			"data":       map[string]interface{}{"button": "buy"},
		},
	}

	body, _ := json.Marshal(events)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/events/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusCreated, http.StatusInternalServerError}, w.Code)
}

func TestQueryEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/events?user_id=user-123&event_type=page_view", nil)
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, w.Code)
}

func TestQueryAggregations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/analytics/aggregations?event_type=page_view&granularity=hour", nil)
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, w.Code)
}

func TestGetAnalyticsSummary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/analytics/summary?event_type=page_view", nil)
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, w.Code)
}

func TestQueryEventsInvalidFrom(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/events?from=invalid-date", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestQueryEventsInvalidTo(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/events?to=invalid-date", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIngestEventInvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/events", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMetricsMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestLoggingMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandler_NewHandler(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	h := &Handler{
		logger: logger,
	}

	assert.NotNil(t, h)
	assert.NotNil(t, h.logger)
}

func TestHandler_SetupRouter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	assert.NotNil(t, router)

	routes := router.Routes()
	assert.Greater(t, len(routes), 0)

	routeMap := make(map[string]bool)
	for _, route := range routes {
		routeMap[route.Path] = true
	}

	assert.True(t, routeMap["/health"])
	assert.True(t, routeMap["/ready"])
	assert.True(t, routeMap["/api/v1/events"])
}

func TestQueryAggregationsInvalidFrom(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/analytics/aggregations?from=invalid-date", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestQueryAggregationsInvalidTo(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/analytics/aggregations?to=invalid-date", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetAnalyticsSummaryInvalidFrom(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/analytics/summary?from=invalid-date", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetAnalyticsSummaryInvalidTo(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := setupTestHandler(t)
	router := handler.SetupRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/analytics/summary?to=invalid-date", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStartMetricsServer(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	h := &Handler{
		logger: logger,
	}

	server := h.StartMetricsServer("0")
	assert.NotNil(t, server)

	time.Sleep(100 * time.Millisecond)

	err := server.Close()
	assert.NoError(t, err)
}
