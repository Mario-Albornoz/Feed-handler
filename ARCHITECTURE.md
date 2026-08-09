# Architecture Documentation

## Design Patterns

### 1. Pipeline Pattern (Tick Processing)

The `internal/processing` package uses a **Pipeline Pattern** for processing ticks. Each step in the pipeline is an independent processor that can be tested, reordered, or replaced without affecting other steps.

#### Structure

```go
// Core interface
type TickProcessor interface {
    Process(ctx context.Context, state *ProcessingState) error
}

// State flows through pipeline
type ProcessingState struct {
    Tick            *model.RawTick
    SessionBucket   model.SessionBucket
    InstrumentState *model.InstrumentState
    // ... derived metrics, z-scores, flags
    Vector          *model.NormalizedVector
}
```

#### Pipeline Stages

1. **EquityFilterProcessor** - Filters non-equity instruments (returns `ErrSkipTick`)
2. **SessionResolverProcessor** - Determines session bucket (PreMarket, Open, etc.)
3. **InstrumentLookupProcessor** - Gets or creates instrument state from registry
4. **MetricsCalculatorProcessor** - Computes intertick, spread, priceStep, volume
5. **StatsUpdaterProcessor** - Updates both session and all-session statistics
6. **FallbackSelectorProcessor** - Selects session-specific or fallback stats
7. **ZScoreCalculatorProcessor** - Computes fast and slow z-scores
8. **CusumExtractorProcessor** - Extracts CUSUM values from stats
9. **FlagCalculatorProcessor** - Computes GapFlag, QuoteInv, WarmupFlag
10. **VectorBuilderProcessor** - Constructs the NormalizedVector
11. **VectorEmitterProcessor** - Writes vector to Kafka
12. **StateUpdaterProcessor** - Updates LastTickTime and PrevMid

#### Benefits

✅ **Testability** - Each processor can be unit tested in isolation
✅ **Extensibility** - Add new processors with `AddProcessor()` or `InsertProcessor()`
✅ **Clarity** - Each step has a single, clear responsibility
✅ **Debugging** - Easy to identify which stage failed
✅ **Reusability** - Processors can be reused in different pipelines

#### Example: Adding a Custom Processor

```go
type CustomValidator struct{}

func (v *CustomValidator) Process(ctx context.Context, state *ProcessingState) error {
    if state.CurrentMid < 0 {
        return errors.New("invalid negative price")
    }
    return nil
}

// Add to pipeline
processor := NewFeedProcessor(...)
processor.InsertProcessor(4, &CustomValidator{}) // Insert after MetricsCalculator
```

---

### 2. Builder Pattern (System Initialization)

The `cmd/aggregator` package uses a **Builder Pattern** for system initialization. This provides a fluent API for constructing the system with proper error handling and dependency ordering.

#### Structure

```go
type SystemBuilder struct {
    config    *config.AggregatorConfig
    resolver  *model.SessionResolver
    registry  *model.InstrumentRegistry
    producer  *kafka.Producer
    processor *processing.FeedProcessor
    consumer  *kafka.FeedConsumer
    detector  *silence.Detector
    err       error
}
```

#### Usage

```go
system, err := NewSystemBuilder().
    WithConfigPath(configPath).
    WithConfig().
    WithSessionResolver().
    WithRegistry().
    WithProducer().
    WithProcessor().
    WithConsumer().
    WithDetector().
    Build()

if err != nil {
    log.Fatalf("Failed to build system: %v", err)
}

defer system.Shutdown()
system.Run(context.Background())
```

#### Benefits

✅ **Fluent API** - Readable, self-documenting initialization sequence
✅ **Error Propagation** - First error stops the chain, no need for error checking at each step
✅ **Extensibility** - Easy to add new components with new `WithXXX()` methods
✅ **Testability** - Can create different configurations (dev, test, prod)
✅ **Dependency Management** - Order enforced by method chaining

#### Example: Adding a New Component

```go
// In system.go
func (b *SystemBuilder) WithMetricsExporter() *SystemBuilder {
    if b.err != nil {
        return b
    }
    
    log.Println("Creating metrics exporter...")
    b.metricsExporter = NewMetricsExporter(b.config.Metrics)
    return b
}

// In main.go
system, err := NewSystemBuilder().
    WithConfig().
    WithMetricsExporter().  // ← New component
    // ... rest of chain
    Build()
```

---

## Component Interaction

```
main.go
  ├─ SystemBuilder
  │   ├─ Config Loader
  │   ├─ SessionResolver
  │   ├─ InstrumentRegistry
  │   ├─ Kafka Producer
  │   ├─ FeedProcessor (Pipeline)
  │   ├─ Kafka Consumer
  │   └─ Silence Detector
  └─ System.Run()

System.Run() starts:
  ├─ Consumer Goroutine
  │   └─ Reads from Kafka
  │       └─ FeedProcessor.ProcessRawTicks()
  │           └─ Executes Pipeline (12 stages)
  └─ Detector Goroutine
      └─ Scans Registry every N seconds
          └─ Emits Silence Alerts
```

---

## Data Flow

### Tick Processing Flow

```
RawTick → Pipeline → NormalizedVector
  ↓
  1. Filter (SecType == "E"?)
  2. Resolve Session (PreMarket, Open, etc.)
  3. Lookup Instrument (GetOrCreate from registry)
  4. Calculate Metrics (intertick, spread, priceStep, volume)
  5. Update Stats (session-specific + all-sessions)
  6. Select Stats (fallback if < MinObservations)
  7. Calculate Z-Scores (fast + slow baselines)
  8. Extract CUSUM (accumulated deviations)
  9. Calculate Flags (GapFlag, QuoteInv, WarmupFlag)
  10. Build Vector (package all data)
  11. Emit Vector (write to Kafka)
  12. Update State (LastTickTime, PrevMid)
```

### State Management

```
ProcessingState (per tick)
  ├─ Immutable: Tick, SessionBucket, InstrumentKey
  ├─ Mutable: Metrics, Scores, Flags
  └─ Output: Vector

InstrumentState (shared across ticks)
  ├─ LastTickTime (updated at end)
  ├─ PrevMid (updated at end)
  ├─ StatsBySession (updated during pipeline)
  └─ AllSessionStats (updated during pipeline)
```

---

## Extension Points

### Adding a Processing Step

1. Create a new processor implementing `TickProcessor`
2. Add to pipeline in `NewFeedProcessor()`
3. Update `ProcessingState` if new fields needed
4. Write unit test for the processor

### Adding a System Component

1. Add field to `System` struct
2. Add field to `SystemBuilder` struct
3. Add `WithComponentName()` method
4. Update `Build()` to include new component
5. Update `System.Shutdown()` if cleanup needed

### Adding Configuration

1. Update `config.AggregatorConfig` struct
2. Add to `config/aggregator.yaml`
3. Add validation to `config.Validate()`
4. Use in relevant processor/component

---

## Testing Strategy

### Unit Tests

- **Pipeline Processors**: Test each processor with mock state
- **Builder**: Test each `WithXXX()` method independently
- **Integration**: Test full pipeline with real components

### Example: Testing a Processor

```go
func TestMetricsCalculator(t *testing.T) {
    processor := &MetricsCalculatorProcessor{}
    
    state := &ProcessingState{
        Tick: &model.RawTick{
            Bid: 100.0,
            Ask: 100.5,
        },
        InstrumentState: &model.InstrumentState{
            PrevMid: 99.0,
        },
    }
    
    err := processor.Process(context.Background(), state)
    assert.NoError(t, err)
    assert.Equal(t, 0.5, state.Spread)
    assert.Equal(t, 100.25, state.CurrentMid)
}
```

---

## Performance Considerations

### Pipeline Overhead

- **Allocation**: One `ProcessingState` per tick (stack-allocated)
- **Function calls**: 12 interface calls per tick
- **Trade-off**: ~1% overhead for massive maintainability gain

### Builder Overhead

- **Startup only**: No runtime cost
- **Memory**: Temporary builder struct discarded after `Build()`

---

## Comparison: Before vs After

### Before (Monolithic)

```go
func (fp *FeedProcessor) ProcessRawTicks(...) error {
    // 150 lines of logic all in one function
    // Hard to test individual steps
    // Difficult to add new logic
    // State implicit and hard to trace
}
```

### After (Pipeline)

```go
func (fp *FeedProcessor) ProcessRawTicks(...) error {
    state := &ProcessingState{Tick: rawTick}
    for _, processor := range fp.pipeline {
        if err := processor.Process(ctx, state); err != nil {
            return err
        }
    }
    return nil
}
```

**Result**: 12 lines of orchestration code + 12 focused, testable processors

---

## Lessons Learned

1. **Pipeline Pattern** excels for multi-step data transformations
2. **Builder Pattern** simplifies complex initialization sequences
3. **Explicit state** (ProcessingState) makes data flow transparent
4. **Interface-based design** enables easy testing and mocking
5. **Small, focused components** are easier to reason about and maintain

---

## Future Enhancements

### Potential Additions

1. **Conditional Processors**: Skip certain steps based on instrument type
2. **Parallel Processors**: Run independent steps concurrently
3. **Processor Metrics**: Track time spent in each pipeline stage
4. **Pipeline Validation**: Ensure dependencies between processors
5. **Hot-reload Processors**: Swap processors without restart
