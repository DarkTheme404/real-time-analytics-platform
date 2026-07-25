package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_events_ingested_total",
			Help: "Total number of events ingested",
		},
		[]string{"source", "event_type"},
	)

	EventsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_events_processed_total",
			Help: "Total number of events processed",
		},
		[]string{"status"},
	)

	EventsBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "analytics_events_batch_size",
			Help:    "Size of event batches written to ClickHouse",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
	)

	QueryDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "analytics_query_duration_seconds",
			Help:    "Duration of analytics queries",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"query_type"},
	)

	CacheHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_cache_hits_total",
			Help: "Total number of cache hits",
		},
		[]string{"cache_type"},
	)

	CacheMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_cache_misses_total",
			Help: "Total number of cache misses",
		},
		[]string{"cache_type"},
	)

	KafkaConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "analytics_kafka_consumer_lag",
			Help: "Kafka consumer lag per partition",
		},
		[]string{"topic", "partition"},
	)

	WorkersActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "analytics_workers_active",
			Help: "Number of active aggregation workers",
		},
	)
)
