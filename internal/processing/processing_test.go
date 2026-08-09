package processing

import (
	"context"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

// MockVectorEmitter mocks VectorEmitter for testing
type MockVectorEmitter struct {
	vectors      []*model.NormalizedVector
	shouldFail   bool
}

func (m *MockVectorEmitter) WriteVector(ctx context.Context, vector *model.NormalizedVector) error {
	if m.shouldFail {
		return &mockError{}
	}
	m.vectors = append(m.vectors, vector)
	return nil
}

type mockError struct{}

func (e *mockError) Error() string {
	return "mock error"
}

func setupTestProcessor(t *testing.T) (*FeedProcessor, *model.InstrumentRegistry, *MockVectorEmitter) {
	cfg := config.AggregatorConfig{
		Windows: config.WindowConfig{
			FastWindowTicks: 50,
			SlowWindowTicks: 200,
		},
		CUSUM: config.CUSUMConfig{
			Slack: 0.5,
		},
	}

	// Create session resolver with test exchanges
	loc, _ := time.LoadLocation("America/New_York")
	exchangeHours := &model.ExchangeHours{
		Timezone:        loc,
		PreMarketStart:  4 * time.Hour,
		MarketOpen:      9*time.Hour + 30*time.Minute,
		MiddayStart:     12 * time.Hour,
		CloseStart:      15*time.Hour + 30*time.Minute,
		MarketClose:     16 * time.Hour,
		AfterHoursEnd:   20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}
	exchanges := map[string]*model.ExchangeHours{
		"NYSE":   exchangeHours,
		"NASDAQ": exchangeHours, // Same hours for simplicity
	}
	resolver := model.NewSessionResolver(exchanges)

	registry := model.NewInstrumentRegistry(cfg.Windows.FastWindowTicks, cfg.Windows.SlowWindowTicks, cfg.CUSUM.Slack)
	emitter := &MockVectorEmitter{}

	processor := NewFeedProcessor(cfg, resolver, registry, emitter)

	return processor, registry, emitter
}

func TestProcessRawTicks_EquityFilter(t *testing.T) {
	processor, _, emitter := setupTestProcessor(t)

	// Test non-equity instrument (should be skipped)
	tick := &model.RawTick{
		ID:          "SPX",
		Exchange:    "NYSE",
		SecType:     "I", // Index, not equity
		Bid:         4500.0,
		Ask:         4500.5,
		TotalVolume: 0,
		TradingTime: time.Now(),
	}

	err := processor.ProcessRawTicks(context.Background(), tick)
	if err != nil {
		t.Fatalf("ProcessRawTicks failed: %v", err)
	}

	// Should not emit vector for non-equity
	if len(emitter.vectors) != 0 {
		t.Errorf("Expected 0 vectors for non-equity instrument, got %d", len(emitter.vectors))
	}
}

func TestProcessRawTicks_FirstTick(t *testing.T) {
	processor, registry, emitter := setupTestProcessor(t)

	tick := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.0,
		Ask:         150.5,
		TotalVolume: 1000,
		TradingTime: time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC), // Monday 10 AM
	}

	err := processor.ProcessRawTicks(context.Background(), tick)
	if err != nil {
		t.Fatalf("ProcessRawTicks failed: %v", err)
	}

	// Should emit one vector
	if len(emitter.vectors) != 1 {
		t.Fatalf("Expected 1 vector, got %d", len(emitter.vectors))
	}

	vector := emitter.vectors[0]

	// Verify basic fields
	if vector.Exchange != "NYSE" {
		t.Errorf("Expected exchange NYSE, got %s", vector.Exchange)
	}
	if vector.Instrument != "AAPL" {
		t.Errorf("Expected instrument AAPL, got %s", vector.Instrument)
	}
	if vector.Class != "E" {
		t.Errorf("Expected class E, got %s", vector.Class)
	}

	// First tick should have warmup flag set
	if vector.WarmupFlag != 1 {
		t.Errorf("Expected warmup flag 1 for first tick, got %d", vector.WarmupFlag)
	}

	// Check instrument state was updated
	key := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "AAPL",
	}
	state := registry.GetOrCreate(key)
	
	if state.LastTickTime.IsZero() {
		t.Error("LastTickTime should be set after processing")
	}
	
	expectedMid := (150.0 + 150.5) / 2.0
	if state.PrevMid != expectedMid {
		t.Errorf("Expected PrevMid %f, got %f", expectedMid, state.PrevMid)
	}
}

func TestProcessRawTicks_IntertickCalculation(t *testing.T) {
	processor, registry, _ := setupTestProcessor(t)

	baseTime := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)

	// First tick
	tick1 := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.0,
		Ask:         150.5,
		TotalVolume: 1000,
		TradingTime: baseTime,
	}

	err := processor.ProcessRawTicks(context.Background(), tick1)
	if err != nil {
		t.Fatalf("First ProcessRawTicks failed: %v", err)
	}

	// Second tick 100ms later
	tick2 := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.1,
		Ask:         150.6,
		TotalVolume: 1100,
		TradingTime: baseTime.Add(100 * time.Millisecond),
	}

	err = processor.ProcessRawTicks(context.Background(), tick2)
	if err != nil {
		t.Fatalf("Second ProcessRawTicks failed: %v", err)
	}

	// The intertick value is internal to the update, but we can verify
	// that the rolling stats were updated (ObservationCount should be 2)
	key := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "AAPL",
	}
	state := registry.GetOrCreate(key)

	// All-session stats should have 2 observations now
	if state.AllSessionStats.ObservationCount != 2 {
		t.Errorf("Expected 2 observations, got %d", state.AllSessionStats.ObservationCount)
	}
}

func TestProcessRawTicks_SpreadCalculation(t *testing.T) {
	processor, _, emitter := setupTestProcessor(t)

	tick := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.0,
		Ask:         151.0, // Spread of 1.0
		TotalVolume: 1000,
		TradingTime: time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC),
	}

	err := processor.ProcessRawTicks(context.Background(), tick)
	if err != nil {
		t.Fatalf("ProcessRawTicks failed: %v", err)
	}

	// Spread is calculated and used in stats update, but not directly exposed
	// We can verify the vector was created
	if len(emitter.vectors) != 1 {
		t.Fatalf("Expected 1 vector, got %d", len(emitter.vectors))
	}
}

func TestProcessRawTicks_QuoteInvFlag(t *testing.T) {
	processor, _, emitter := setupTestProcessor(t)

	// Normal quote
	tick1 := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.0,
		Ask:         150.5,
		TotalVolume: 1000,
		TradingTime: time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC),
	}

	err := processor.ProcessRawTicks(context.Background(), tick1)
	if err != nil {
		t.Fatalf("ProcessRawTicks failed: %v", err)
	}

	if emitter.vectors[0].QuoteInv != 0 {
		t.Errorf("Expected QuoteInv=0 for normal quote, got %d", emitter.vectors[0].QuoteInv)
	}

	// Inverted quote (bid >= ask)
	tick2 := &model.RawTick{
		ID:          "GOOGL",
		Exchange:    "NASDAQ",
		SecType:     "E",
		Bid:         2800.5,
		Ask:         2800.0, // Inverted!
		TotalVolume: 500,
		TradingTime: time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC),
	}

	err = processor.ProcessRawTicks(context.Background(), tick2)
	if err != nil {
		t.Fatalf("ProcessRawTicks failed: %v", err)
	}

	if emitter.vectors[1].QuoteInv != 1 {
		t.Errorf("Expected QuoteInv=1 for inverted quote, got %d", emitter.vectors[1].QuoteInv)
	}
}

func TestProcessRawTicks_SessionFallback(t *testing.T) {
	processor, _, emitter := setupTestProcessor(t)

	// Create a few ticks for the same instrument
	baseTime := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC) // Monday 10 AM

	for i := 0; i < 5; i++ {
		tick := &model.RawTick{
			ID:          "AAPL",
			Exchange:    "NYSE",
			SecType:     "E",
			Bid:         150.0 + float64(i)*0.1,
			Ask:         150.5 + float64(i)*0.1,
			TotalVolume: 1000,
			TradingTime: baseTime.Add(time.Duration(i) * 100 * time.Millisecond),
		}

		err := processor.ProcessRawTicks(context.Background(), tick)
		if err != nil {
			t.Fatalf("ProcessRawTicks tick %d failed: %v", i, err)
		}
	}

	// After 5 ticks, should still be in warmup (MinObservations is 50)
	// so SessionFallbackFlag should be 1
	lastVector := emitter.vectors[len(emitter.vectors)-1]
	if lastVector.SessionFallbackFlag != 1 {
		t.Errorf("Expected SessionFallbackFlag=1 during warmup, got %d", lastVector.SessionFallbackFlag)
	}
	if lastVector.WarmupFlag != 1 {
		t.Errorf("Expected WarmupFlag=1 during warmup, got %d", lastVector.WarmupFlag)
	}
}

func TestProcessRawTicks_MultipleInstruments(t *testing.T) {
	processor, registry, emitter := setupTestProcessor(t)

	instruments := []string{"AAPL", "GOOGL", "MSFT"}
	baseTime := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)

	for i, inst := range instruments {
		tick := &model.RawTick{
			ID:          inst,
			Exchange:    "NYSE",
			SecType:     "E",
			Bid:         100.0 + float64(i),
			Ask:         100.5 + float64(i),
			TotalVolume: 1000,
			TradingTime: baseTime.Add(time.Duration(i) * time.Second),
		}

		err := processor.ProcessRawTicks(context.Background(), tick)
		if err != nil {
			t.Fatalf("ProcessRawTicks for %s failed: %v", inst, err)
		}
	}

	// Should have 3 vectors emitted
	if len(emitter.vectors) != 3 {
		t.Fatalf("Expected 3 vectors, got %d", len(emitter.vectors))
	}

	// Verify each instrument has its own state
	for i, inst := range instruments {
		key := model.InstrumentKey{
			Source:               "NYSE",
			InstrumentIdentifier: inst,
		}
		state := registry.GetOrCreate(key)
		
		if state.AllSessionStats.ObservationCount != 1 {
			t.Errorf("Instrument %s: expected 1 observation, got %d", inst, state.AllSessionStats.ObservationCount)
		}
		
		if emitter.vectors[i].Instrument != inst {
			t.Errorf("Vector %d: expected instrument %s, got %s", i, inst, emitter.vectors[i].Instrument)
		}
	}
}

func TestProcessRawTicks_ProducerFailure(t *testing.T) {
	processor, _, emitter := setupTestProcessor(t)
	emitter.shouldFail = true

	tick := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.0,
		Ask:         150.5,
		TotalVolume: 1000,
		TradingTime: time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC),
	}

	err := processor.ProcessRawTicks(context.Background(), tick)
	if err == nil {
		t.Error("Expected error when producer fails, got nil")
	}
}

func TestProcessRawTicks_PriceStepCalculation(t *testing.T) {
	processor, _, emitter := setupTestProcessor(t)

	baseTime := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)

	// First tick
	tick1 := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.0,
		Ask:         150.5,
		TotalVolume: 1000,
		TradingTime: baseTime,
	}

	err := processor.ProcessRawTicks(context.Background(), tick1)
	if err != nil {
		t.Fatalf("First tick failed: %v", err)
	}

	// Second tick with price movement
	tick2 := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         151.0, // Moved up
		Ask:         151.5,
		TotalVolume: 1100,
		TradingTime: baseTime.Add(100 * time.Millisecond),
	}

	err = processor.ProcessRawTicks(context.Background(), tick2)
	if err != nil {
		t.Fatalf("Second tick failed: %v", err)
	}

	// PriceStep was calculated as abs(mid2 - mid1)
	// mid1 = 150.25, mid2 = 151.25, priceStep = 1.0
	// Stats were updated with this value
	if len(emitter.vectors) != 2 {
		t.Fatalf("Expected 2 vectors, got %d", len(emitter.vectors))
	}
}

func TestProcessRawTicks_NoProducer(t *testing.T) {
	cfg := config.AggregatorConfig{
		Windows: config.WindowConfig{
			FastWindowTicks: 50,
			SlowWindowTicks: 200,
		},
		CUSUM: config.CUSUMConfig{
			Slack: 0.5,
		},
	}

	loc, _ := time.LoadLocation("America/New_York")
	exchanges := map[string]*model.ExchangeHours{
		"NYSE": {
			Timezone:        loc,
			PreMarketStart:  4 * time.Hour,
			MarketOpen:      9*time.Hour + 30*time.Minute,
			MiddayStart:     12 * time.Hour,
			CloseStart:      15*time.Hour + 30*time.Minute,
			MarketClose:     16 * time.Hour,
			AfterHoursEnd:   20 * time.Hour,
			TradingWeekdays: map[time.Weekday]bool{
				time.Monday: true, time.Tuesday: true, time.Wednesday: true,
				time.Thursday: true, time.Friday: true,
			},
		},
	}
	resolver := model.NewSessionResolver(exchanges)
	registry := model.NewInstrumentRegistry(cfg.Windows.FastWindowTicks, cfg.Windows.SlowWindowTicks, cfg.CUSUM.Slack)

	// No producer (nil)
	processor := NewFeedProcessor(cfg, resolver, registry, nil)

	tick := &model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.0,
		Ask:         150.5,
		TotalVolume: 1000,
		TradingTime: time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC),
	}

	// Should not error when producer is nil
	err := processor.ProcessRawTicks(context.Background(), tick)
	if err != nil {
		t.Errorf("Expected no error with nil producer, got %v", err)
	}
}
