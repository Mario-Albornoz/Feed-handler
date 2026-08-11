# Integration Tests

End-to-end integration tests for the feed-handler-aggregator using local Kafka.

## Prerequisites

- Docker and Docker Compose installed
- Go 1.22+

## Running the Tests

### 1. Start Kafka

```bash
docker-compose -f docker-compose.test.yml up -d
```

Wait ~30 seconds for Kafka to fully initialize. You can check the logs:

```bash
docker-compose -f docker-compose.test.yml logs -f kafka
```

### 2. Run Integration Tests

```bash
INTEGRATION_TEST=1 go test ./test/integration/... -v -timeout=5m
```

**Note:** These tests require the aggregator to be running separately for the silence detection test. For other tests (normal flow, partition keys, quote inversion), the synthetic generator directly validates Kafka output.

### 3. Run Specific Tests

```bash
# Normal flow only
INTEGRATION_TEST=1 go test ./test/integration/... -v -run TestEndToEndNormalFlow

# Partition key consistency
INTEGRATION_TEST=1 go test ./test/integration/... -v -run TestPartitionKeyConsistency

# Quote inversion detection
INTEGRATION_TEST=1 go test ./test/integration/... -v -run TestQuoteInversionDetection

# Skip slow silence test
INTEGRATION_TEST=1 go test ./test/integration/... -v -short
```

### 4. Stop Kafka

```bash
docker-compose -f docker-compose.test.yml down -v
```

## Test Coverage

### TestEndToEndNormalFlow
- Generates 50 normal equity ticks at 50ms intervals
- Validates `NormalizedVector` output on correct topic
- Verifies exchange and instrument fields
- Checks partition key format: `{exchange}:{model_key}`

### TestPartitionKeyConsistency (AGG-4 Regression)
- Tests 3 instruments (AAPL, GOOGL, MSFT) with 10 ticks each
- Validates that each `{exchange}:{model_key}` always routes to the same partition
- Catches partition key regressions

### TestQuoteInversionDetection (Phase 4)
- Sends 20 normal ticks followed by 10 inverted quotes (bid >= ask)
- Validates `QuoteInversionFlag` is set to 1 for inverted quotes

### TestSilenceDetection (AGG-5 Regression)
- Generates 30 normal ticks at 100ms intervals
- Simulates 5-second silence gap
- Validates `SilenceAlert` is emitted to health-events topic
- Checks alert partition key: `{exchange}:SILENCE`
- **Requires aggregator running with silence detector enabled**

## Architecture

```
[TickGenerator] → [Kafka: raw-ticks] → [Aggregator] → [Kafka: normalized-vectors]
                                                     → [Kafka: health-events]
```

The tests use:
- `generator.go`: Synthetic tick generation (normal, drift, inversion, silence scenarios)
- `integration_test.go`: End-to-end validation logic
- `docker-compose.test.yml`: Isolated Kafka cluster (Zookeeper + single broker)

## CI Integration

To run in CI:

```yaml
# Example GitHub Actions
- name: Start Kafka
  run: docker-compose -f docker-compose.test.yml up -d

- name: Wait for Kafka
  run: sleep 30

- name: Run Integration Tests
  run: INTEGRATION_TEST=1 go test ./test/integration/... -v -timeout=5m -short

- name: Stop Kafka
  run: docker-compose -f docker-compose.test.yml down -v
```

Use `-short` flag to skip the slow silence detection test in CI unless the aggregator is also deployed in the pipeline.

## Troubleshooting

**"Failed to connect to Kafka"**
- Ensure Docker Compose is running: `docker ps`
- Check Kafka health: `docker-compose -f docker-compose.test.yml logs kafka`
- Wait longer for Kafka to initialize (can take 30-60s on first run)

**"Timeout: No normalized vectors received"**
- Verify topics exist: `docker exec -it test-kafka kafka-topics --bootstrap-server localhost:9092 --list`
- Check aggregator is running (for full end-to-end tests)
- Verify config points to correct broker/topics

**"Partition key mismatch"**
- This indicates a regression in AGG-4 (producer partition key logic)
- Check `internal/kafka/producer.go` WriteVector/WriteAlert methods

## Known Limitations

- Silence detection test requires aggregator process running (not purely synthetic)
- Tests use single Kafka broker (replication factor 1)
- No DST/session boundary tests yet (covered by unit tests in `internal/model/session_test.go`)
