# Real-time Analytics Platform

A production-ready real-time analytics platform built with Go, ClickHouse, Kafka, and Redis.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Real-time Analytics Platform                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   Clients    │    │   Clients    │    │   Clients    │                  │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                  │
│         │                   │                   │                           │
│         └───────────────────┼───────────────────┘                           │
│                             │                                               │
│                    ┌────────▼────────┐                                      │
│                    │   Load Balancer │                                      │
│                    └────────┬────────┘                                      │
│                             │                                               │
│                    ┌────────▼────────┐                                      │
│                    │    API Server   │◄──── Redis Cache                     │
│                    │   (REST API)    │────► ClickHouse                      │
│                    └────────┬────────┘                                      │
│                             │                                               │
│                    ┌────────▼────────┐                                      │
│                    │  Apache Kafka   │                                      │
│                    │  (Message Bus)  │                                      │
│                    └────────┬────────┘                                      │
│                             │                                               │
│              ┌──────────────┼──────────────┐                               │
│              │              │              │                                │
│     ┌────────▼───────┐ ┌───▼────────────┐ ┌▼──────────────┐              │
│     │   Ingester     │ │    Worker      │ │   Aggregator  │              │
│     │ (Event Proc.)  │ │ (Aggregation)  │ │   (Analytics) │              │
│     └────────┬───────┘ └───┬────────────┘ └┬──────────────┘              │
│              │              │              │                                │
│              └──────────────┼──────────────┘                               │
│                             │                                               │
│                    ┌────────▼────────┐                                      │
│                    │   ClickHouse    │                                      │
│                    │  (Analytics DB) │                                      │
│                    └─────────────────┘                                      │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        Monitoring Stack                              │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                │   │
│  │  │ Prometheus  │  │   Grafana   │  │   Alerts    │                │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### Services

| Service | Description | Port |
|---------|-------------|------|
| `ingester` | Kafka consumer that processes events into ClickHouse | 9090 (metrics) |
| `api` | HTTP API server for analytics queries with Redis caching | 8080 (HTTP), 9090 (metrics) |
| `worker` | Aggregation worker, produces back to Kafka | 9090 (metrics) |

### Infrastructure

- **ClickHouse**: Column-oriented DBMS for analytics
- **Apache Kafka**: Distributed event streaming platform
- **Redis**: In-memory data structure store for caching
- **Prometheus**: Monitoring and alerting
- **Grafana**: Visualization and dashboards

## Quick Start

### Prerequisites

- Go 1.22+
- Docker and Docker Compose
- kubectl (for Kubernetes deployment)
- Terraform (for infrastructure)

### Local Development

```bash
# Clone the repository
git clone https://github.com/DarkTheme404/real-time-analytics-platform.git
cd real-time-analytics-platform

# Start local environment
make docker-up

# View logs
make docker-logs

# Stop environment
make docker-down
```

### API Usage

```bash
# Health check
curl http://localhost:8080/health

# Ingest an event
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "page_view",
    "user_id": "user-123",
    "source": "web",
    "data": {"page": "/home"}
  }'

# Query events
curl "http://localhost:8080/api/v1/events?user_id=user-123&event_type=page_view"

# Get aggregations
curl "http://localhost:8080/api/v1/analytics/aggregations?event_type=page_view&granularity=hour"

# Get analytics summary
curl "http://localhost:8080/api/v1/analytics/summary?event_type=page_view"
```

## Development

### Build

```bash
make build
```

### Test

```bash
make test
make test-coverage
```

### Lint

```bash
make lint
```

### Format

```bash
make fmt
```

## Deployment

### Kubernetes

```bash
# Apply configurations
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/hpa.yaml

# Check status
kubectl get pods -l component=ingester
kubectl get pods -l component=api
kubectl get pods -l component=worker
```

### Terraform

```bash
cd deploy/terraform

# Initialize
terraform init

# Plan
terraform plan -var="project_id=your-project-id"

# Apply
terraform apply -var="project_id=your-project-id"
```

## Monitoring

### Grafana Dashboard

Access Grafana at `http://localhost:3000` (admin/admin)

Dashboard includes:
- Events Ingested (rate by source and type)
- Events Processed (rate by status)
- Query Duration (p50, p95, p99)
- Cache Hit Rate
- Batch Size Distribution
- Active Workers
- Kafka Consumer Lag

### Prometheus

Access Prometheus at `http://localhost:9091`

Available metrics:
- `analytics_events_ingested_total`
- `analytics_events_processed_total`
- `analytics_events_batch_size`
- `analytics_query_duration_seconds`
- `analytics_cache_hits_total`
- `analytics_cache_misses_total`
- `analytics_kafka_consumer_lag`
- `analytics_workers_active`

## Configuration

All configuration is environment-based. See `.env.example` for available options.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `analytics-events` | Kafka topic for events |
| `KAFKA_CONSUMER_GROUP` | `analytics-platform` | Kafka consumer group |
| `CLICKHOUSE_DSN` | `tcp://localhost:9000/analytics` | ClickHouse connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `API_PORT` | `8080` | API server port |
| `METRICS_PORT` | `9090` | Metrics server port |
| `LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |

## CI/CD

GitHub Actions pipeline with:
1. Lint (golangci-lint, go vet, formatting)
2. Test (unit tests with coverage)
3. Build (Docker images)
4. Deploy (Kubernetes)

## Architecture Decisions

### Event Processing Pipeline

1. **Ingestion**: Events are received via HTTP API and published to Kafka
2. **Processing**: Ingester consumes events and writes to ClickHouse
3. **Aggregation**: Worker processes events and creates aggregations
4. **Querying**: API serves analytics queries with Redis caching

### Storage Choices

- **ClickHouse**: Optimized for analytical queries on large datasets
- **Redis**: Fast caching for frequently accessed queries
- **Kafka**: Reliable event streaming with consumer groups

### Scalability

- Horizontal scaling via Kubernetes HPA
- Kafka consumer groups for parallel processing
- ClickHouse sharding support
- Redis clustering support

## License

MIT License
