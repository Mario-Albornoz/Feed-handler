# Feed Handler Aggregator

A high-throughput market data aggregation system that normalizes raw ticks into feature vectors using learned per-instrument, per-session baselines with no static thresholds.

## Architecture Overview

The aggregator processes raw market ticks and:
1. Maintains two-timescale exponential moving averages (fast ~60 ticks, slow ~14,400 ticks)
2. Computes z-scores and CUSUM values for anomaly detection
3. Monitors feed silence using proportional thresholds (learned per instrument)
4. Emits normalized vectors and silence alerts to Kafka

**Key Design Principle**: No hardcoded thresholds. Every instrument learns its own expected behavior from observations. A liquid equity ticking every 2ms and a sparse options contract ticking every 4 hours are both monitored using the same code and same parameters.

## Features

- ✅ **AGG-1a**: Session/time-bucket resolution with DST handling
- ✅ **AGG-1b**: Illiquid-instrument fallback (session-specific → all-sessions)
- ✅ **AGG-2**: Thread-safe instrument registry with persistence (GOB format)
- ✅ **AGG-3**: Kafka consumer with graceful shutdown
- ✅ **AGG-4**: Kafka producer with partition key routing
- ✅ **AGG-5**: Silence detector with learned thresholds
- ✅ **AGG-6**: Config loader with validation
- ✅ **AGG-7**: Main wiring with signal handling
- ✅ **AGG-8**: Integration tests with Docker Compose

## Prerequisites

- Go 1.22+
- Kafka cluster (local or remote)
- Input topic with raw tick data

## Configuration

Edit `config/aggregator.yaml`:

```yaml
kafka:
  brokers:
    - "localhost:9092"
  input_topic: "raw-ticks"
  output_topic: "normalized-vectors"
  alert_topic: "health-events"
  consumer_group: "aggregator-group"

windows:
  fast_window_ticks: 60       # Fast baseline window in ticks
  slow_window_ticks: 14400    # Slow baseline window in ticks

cusum:
  slack: 0.5                  # CUSUM drift allowance
  threshold: 5.0              # CUSUM alert threshold

silence:
  check_interval_sec: 5       # How often to check for silence
  gap_multiplier: 5.0         # Alert when elapsed > 5x learned mean

exchanges:
  NYSE:
    timezone: "America/New_York"
    premarket_start: "04:00"
    market_open: "09:30"
    midday_start: "12:00"
    close_start: "15:30"
    market_close: "16:00"
    afterhours_end: "20:00"
    trading_weekdays: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
```

## Building

```bash
# Build binary
go build -o aggregator ./cmd/aggregator

# Or use go run
go run ./cmd/aggregator
```

## Running

```bash
# Use default config path (config/aggregator.yaml)
./aggregator

# Specify custom config path
./aggregator /path/to/config.yaml

# Or via environment variable
AGGREGATOR_CONFIG=/path/to/config.yaml ./aggregator

# Override registry path (default: data/registry.gob)
REGISTRY_PATH=/path/to/registry.gob ./aggregator
```

## Data Flow

```
Raw Ticks (Kafka)
    ↓
Consumer (AGG-3)
    ↓
FeedProcessor (processes each tick)
    ├─→ Session Resolution (AGG-1a)
    ├─→ Registry Lookup/Create (AGG-2)
    ├─→ Stats Update (fast + slow EMAs)
    ├─→ Fallback Selection (AGG-1b)
    ├─→ Z-score Calculation
    ├─→ Flag Computation
    └─→ Vector Emission
           ↓
Producer (AGG-4) → Kafka (normalized-vectors topic)

Silence Detector (AGG-5) - Independent goroutine
    ├─→ Scans registry every N seconds
    ├─→ Compares elapsed time vs learned threshold
    └─→ Emits alerts → Kafka (health-events topic)
```

## Graceful Shutdown

Press `Ctrl+C` or send `SIGTERM`:
1. Stops consuming new ticks
2. Cancels silence detector
3. Flushes pending Kafka writes
4. Saves registry to disk (preserves learned baselines)
5. Exits cleanly

On restart, the aggregator loads the saved registry, so instruments that were warm remain warm.

## Testing

### Unit Tests

```bash
# Run all unit tests
go test ./internal/... -v

# With race detection
go test ./internal/... -v -race

# With coverage
go test ./internal/... -v -cover

# Specific package
go test ./internal/processing/... -v

# Specific test
go test -v -run TestProcessRawTicks_FirstTick ./internal/processing/...
```

### Integration Tests

Integration tests validate end-to-end flow using a local Kafka cluster.

```bash
# Using Makefile (recommended)
make kafka-up                # Start Kafka via Docker Compose
make test-integration        # Run integration tests (skips slow tests)
make kafka-down              # Cleanup

# Manual execution
docker-compose -f docker-compose.test.yml up -d
sleep 30  # Wait for Kafka to initialize
INTEGRATION_TEST=1 go test ./test/integration/... -v -short
docker-compose -f docker-compose.test.yml down -v
```

**Integration test coverage:**
- `TestEndToEndNormalFlow` - Basic tick → vector pipeline
- `TestPartitionKeyConsistency` - AGG-4 regression (partition keys)
- `TestQuoteInversionDetection` - Phase 4 quote inversion detection
- `TestSilenceDetection` - AGG-5 regression (requires aggregator running)

See `test/integration/README.md` for full details.

### All Tests

```bash
# Run everything (unit + integration)
make test
```

## Project Structure

```
feed-handler-aggregator/
├── cmd/aggregator/
│   ├── main.go                      # Entry point
│   └── system.go                    # System builder pattern
├── test/integration/                # End-to-end integration tests
│   ├── generator.go                 # Synthetic tick generator
│   ├── integration_test.go          # Test cases
│   └── README.md                    # Test documentation
├── internal/
│   ├── config/                      # Config loading and validation
│   │   ├── config.go
│   │   └── config_test.go
│   ├── kafka/                       # Kafka consumer and producer
│   │   ├── consumer.go
│   │   ├── consumer_test.go
│   │   ├── producer.go
│   │   └── producer_test.go
│   ├── model/                       # Core data structures
│   │   ├── alert.go
│   │   ├── instrument.go
│   │   ├── raw_tick.go
│   │   ├── registry.go
│   │   ├── registry_test.go
│   │   ├── session.go
│   │   ├── session_test.go
│   │   └── vector.go
│   ├── processing/                  # Core tick processing logic
│   │   ├── processing.go
│   │   └── processing_test.go
│   ├── silence/                     # Silence detection (Phase 3)
│   │   └── detector.go
│   └── stats/                       # Rolling statistics (EMA, CUSUM)
│       ├── rolling.go
│       └── rolling_test.go
├── config/
│   └── aggregator.yaml              # Configuration file
├── data/                            # Registry persistence (created at runtime)
│   └── registry.gob
└── go.mod
```

## Performance Characteristics

- **Throughput**: Designed for 700k+ ticks/sec
- **Memory**: Registry grows with unique instruments (typically thousands)
- **Latency**: Sub-millisecond processing per tick
- **Concurrency**: Thread-safe registry with read-optimized double-checked locking

## Monitoring

The aggregator logs:
- Startup configuration
- Registry load/save operations
- Tick processing errors
- Kafka producer/consumer issues
- Graceful shutdown progress

Future: Export Prometheus metrics for:
- Ticks processed/sec
- Vectors emitted/sec
- Alerts emitted/sec
- Registry size
- Per-exchange throughput

## Known Limitations

1. **Warmup Period**: New instruments require `MinObservations` (default 50) ticks before statistics are reliable. During warmup, `WarmupFlag=1` is set.

2. **Never-Ticked Instruments**: Instruments expected to exist but never observed (e.g., new listings) are invisible to silence detection until they send at least one tick.

3. **Baseline Drift During Decline**: If an instrument experiences gradual decline (Phase 1) before complete silence, the silence threshold may be inflated, increasing detection lag.

4. **Multi-Day Drift**: Very slow drift over multiple days may be undetected if it never exceeds CUSUM threshold. This is a documented limitation for Phase 1 detection.

## License

MIT
