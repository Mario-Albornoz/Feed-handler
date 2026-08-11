# Quick Start Guide

Get the feed-handler-aggregator running in 5 minutes.

## 1. Prerequisites

- Go 1.22+ installed
- Docker and Docker Compose installed (for Kafka)
- Git

## 2. Clone and Build

```bash
git clone <your-repo>
cd feed-handler
go build -o aggregator ./cmd/aggregator
```

## 3. Start Kafka (for testing)

```bash
docker compose -f docker-compose.test.yml up -d
sleep 30  # Wait for Kafka to initialize
```

Verify Kafka is running:
```bash
docker ps  # Should see test-kafka and test-zookeeper
```

## 4. Configure

The default config at `config/aggregator.yaml` is preconfigured for the test Kafka cluster:

```yaml
kafka:
  brokers: ["localhost:9092"]
  input_topic: "raw-ticks"
  output_topic: "normalized-vectors"
  alert_topic: "health-events"
  consumer_group: "aggregator-group"
```

## 5. Run the Aggregator

```bash
./aggregator
```

You should see:
```
Loading configuration from config/aggregator.yaml...
Building session resolver...
Initializing instrument registry...
Creating Kafka producer...
Creating feed processor...
Creating Kafka consumer...
Creating silence detector...
System fully operational. Press Ctrl+C to shutdown gracefully.
Kafka consumer started, processing ticks...
Silence detector started
```

## 6. Send Test Ticks

In a new terminal, use the synthetic generator:

```bash
# Run the integration test generator standalone
go run test/integration/generator.go
```

Or manually produce ticks using Kafka CLI:

```bash
docker exec -it test-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic raw-ticks \
  --property "parse.key=true" \
  --property "key.separator=:"

# Then paste (Exchange:Instrument as key):
NYSE:AAPL:{"ID":"AAPL","Exchange":"NYSE","SecType":"E","ISIN":"US0378331005","Bid":150.10,"Ask":150.12,"TotalVolume":1000,"TradingTime":"2026-08-11T10:30:00Z","Date":"2026-08-11T00:00:00Z","Time":"2026-08-11T10:30:00Z"}
```

## 7. Consume Output

In another terminal, read normalized vectors:

```bash
docker exec -it test-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic normalized-vectors \
  --from-beginning \
  --property print.key=true
```

You should see JSON output with z-scores, CUSUM values, and flags.

## 8. Monitor Silence Alerts

```bash
docker exec -it test-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic health-events \
  --from-beginning \
  --property print.key=true
```

## 9. Run Tests

```bash
# Unit tests
go test ./internal/... -v -short

# Integration tests (requires Kafka running)
INTEGRATION_TEST=1 go test ./test/integration/... -v -short
```

## 10. Shutdown

Press `Ctrl+C` in the aggregator terminal. You'll see:

```
Received signal: interrupt, initiating graceful shutdown...
Closing Kafka consumer...
Closing Kafka producer...
Saving registry to data/registry.gob...
Registry saved successfully (N instruments)
Shutdown complete
```

The learned statistics are saved and will be restored on next startup.

## Cleanup

```bash
# Stop Kafka
docker compose -f docker-compose.test.yml down -v

# Remove generated files
rm -rf data/
rm aggregator
```

## Next Steps

- Read `README.md` for architecture details
- Check `ARCHITECTURE.md` for design patterns
- Review `test/integration/README.md` for testing strategies
- Modify `config/aggregator.yaml` to point to your production Kafka cluster
- Add your exchange configurations and trading hours

## Troubleshooting

**"Failed to connect to Kafka"**
- Ensure Docker Compose is running: `docker ps`
- Check logs: `docker compose -f docker-compose.test.yml logs kafka`
- Wait longer for Kafka initialization (30-60s)

**"No ticks being processed"**
- Verify input topic exists and has messages
- Check consumer group is correct
- Ensure tick format matches `RawTick` struct in `internal/model/raw_tick.go`

**"Registry not loading"**
- Normal on first run (no `data/registry.gob` yet)
- After shutdown, verify `data/registry.gob` exists
- Check file permissions

**"Silence detector not alerting"**
- Instruments need warmup period (MinObservations ticks)
- Check `gap_multiplier` config (default 5.0x learned interval)
- Verify `check_interval_sec` is appropriate (default 5s)

## Performance Tips

- For high throughput, increase `fast_window_ticks` and `slow_window_ticks`
- Adjust Kafka producer batching in `internal/kafka/producer.go`
- Monitor registry size and consider implementing eviction if needed
- Use multiple Kafka partitions for parallel processing
