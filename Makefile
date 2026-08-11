.PHONY: help build test test-unit test-integration kafka-up kafka-down clean

help:
	@echo "Available targets:"
	@echo "  build              - Build the aggregator binary"
	@echo "  test               - Run all tests (unit + integration)"
	@echo "  test-unit          - Run unit tests only"
	@echo "  test-integration   - Run integration tests (requires Kafka)"
	@echo "  kafka-up           - Start local Kafka for testing"
	@echo "  kafka-down         - Stop local Kafka"
	@echo "  clean              - Remove binaries and test artifacts"

build:
	@echo "Building aggregator..."
	go build -o aggregator ./cmd/aggregator

test: test-unit test-integration

test-unit:
	@echo "Running unit tests..."
	go test ./internal/... -v -race -cover

test-integration:
	@echo "Running integration tests..."
	@echo "Note: Requires Kafka running (make kafka-up)"
	INTEGRATION_TEST=1 go test ./test/integration/... -v -timeout=5m -short

test-integration-full:
	@echo "Running full integration tests (including silence detection)..."
	@echo "Note: Requires Kafka and aggregator running"
	INTEGRATION_TEST=1 go test ./test/integration/... -v -timeout=10m

kafka-up:
	@echo "Starting Kafka for integration tests..."
	docker compose -f docker-compose.test.yml up -d
	@echo "Waiting 30s for Kafka to initialize..."
	@sleep 30
	@echo "Kafka ready at localhost:9092"

kafka-down:
	@echo "Stopping Kafka..."
	docker compose -f docker-compose.test.yml down -v

kafka-logs:
	docker compose -f docker-compose.test.yml logs -f kafka

clean:
	@echo "Cleaning up..."
	rm -f aggregator
	rm -rf data/
	go clean -testcache
