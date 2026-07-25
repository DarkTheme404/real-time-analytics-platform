APP_NAME := real-time-analytics-platform
MODULE := github.com/DarkTheme404/real-time-analytics-platform

.PHONY: all build test lint clean docker-build docker-up docker-down migrate

all: build

# Build binaries
build:
	go build -o bin/ingester $(MODULE)/cmd/ingester
	go build -o bin/api $(MODULE)/cmd/api
	go build -o bin/worker $(MODULE)/cmd/worker

# Run tests
test:
	go test -v -race -cover ./...

# Run tests with coverage report
test-coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Lint
lint:
	golangci-lint run ./...

# Format code
fmt:
	gofmt -s -w .
	goimports -w .

# Build and push Docker images
docker-build:
	docker build -t $(APP_NAME)-ingester:latest --target ingester .
	docker build -t $(APP_NAME)-api:latest --target api .
	docker build -t $(APP_NAME)-worker:latest --target worker .

# Start local dev environment
docker-up:
	docker-compose up -d

# Stop local dev environment
docker-down:
	docker-compose down

# Start with rebuild
docker-rebuild:
	docker-compose up -d --build

# View logs
docker-logs:
	docker-compose logs -f

# Apply ClickHouse migrations
migrate:
	go run ./cmd/migrate

# Generate protobuf
proto:
	protoc --go_out=. --go-grpc_out=. proto/*.proto

# Security audit
audit:
	govulncheck ./...

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out coverage.html

# Install tools
tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest

# Help
help:
	@echo "Available targets:"
	@echo "  build        - Build all binaries"
	@echo "  test         - Run tests"
	@echo "  test-coverage - Run tests with coverage"
	@echo "  lint         - Run linter"
	@echo "  fmt          - Format code"
	@echo "  docker-build - Build Docker images"
	@echo "  docker-up    - Start dev environment"
	@echo "  docker-down  - Stop dev environment"
	@echo "  docker-rebuild - Rebuild and start"
	@echo "  docker-logs  - View logs"
	@echo "  migrate      - Apply migrations"
	@echo "  audit        - Security audit"
	@echo "  clean        - Clean build artifacts"
	@echo "  tools        - Install dev tools"
