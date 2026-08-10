package silence

import (
	"context"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

// MockAlertEmitter for testing
type MockAlertEmitter struct {
	alerts []*model.SilenceAlert
}

func (m *MockAlertEmitter) WriteAlert(ctx context.Context, alert *model.SilenceAlert) error {
	m.alerts = append(m.alerts, alert)
	return nil
}

func setupTestDetector(t *testing.T) (*Detector, *model.InstrumentRegistry, *MockAlertEmitter) {
	// Create test exchanges
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

	registry := model.NewInstrumentRegistry(50, 200, 0.5)
	emitter := &MockAlertEmitter{}

	detector := NewDetector(registry, resolver, emitter, 5.0, 5*time.Second)

	return detector, registry, emitter
}

func TestDetector_NoAlertBeforeWarmup(t *testing.T) {
	detector, registry, emitter := setupTestDetector(t)

	// Add an instrument that's not warmed up yet
	key := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "AAPL",
	}
	state := registry.GetOrCreate(key)
	state.LastTickTime = time.Now().Add(-10 * time.Second)

	// Scan should not emit alert (not warmed up)
	detector.scan(context.Background())

	if len(emitter.alerts) != 0 {
		t.Errorf("Expected 0 alerts before warmup, got %d", len(emitter.alerts))
	}
}

func TestDetector_AlertAfterSilenceThreshold(t *testing.T) {
	detector, registry, emitter := setupTestDetector(t)

	// Create a warmed-up instrument with expected 100ms interval
	key := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "AAPL",
	}
	state := registry.GetOrCreate(key)

	// Warm up the instrument with 100 observations
	now := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC) // Monday 10 AM
	for i := 0; i < 100; i++ {
		sessionStats := state.StatsBySession[model.Open]
		sessionStats.Update(100.0, 0.5, 0.1, 1000) // 100ms intertick
		state.AllSessionStats.Update(100.0, 0.5, 0.1, 1000)
	}

	// Set last tick time to 10 seconds ago (well beyond 5x threshold of 500ms)
	state.LastTickTime = now.Add(-10 * time.Second)

	// Mock the detector's time to be "now"
	detector.scan(context.Background())

	if len(emitter.alerts) != 1 {
		t.Fatalf("Expected 1 alert, got %d", len(emitter.alerts))
	}

	alert := emitter.alerts[0]
	if alert.Exchange != "NYSE" {
		t.Errorf("Expected exchange NYSE, got %s", alert.Exchange)
	}
	if alert.Instrument != "AAPL" {
		t.Errorf("Expected instrument AAPL, got %s", alert.Instrument)
	}
	if alert.AlertType != "SILENCE" {
		t.Errorf("Expected alert type SILENCE, got %s", alert.AlertType)
	}
	if alert.LatencyLevel != "SEVERE" {
		t.Errorf("Expected SEVERE latency level (10000ms / 100ms = 100x), got %s", alert.LatencyLevel)
	}
}

func TestDetector_NoAlertWithinThreshold(t *testing.T) {
	detector, registry, emitter := setupTestDetector(t)

	// Create a warmed-up instrument
	key := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "AAPL",
	}
	state := registry.GetOrCreate(key)

	// Warm up with 100ms interval
	for i := 0; i < 100; i++ {
		sessionStats := state.StatsBySession[model.Open]
		sessionStats.Update(100.0, 0.5, 0.1, 1000)
		state.AllSessionStats.Update(100.0, 0.5, 0.1, 1000)
	}

	// Set last tick time to only 200ms ago (within 5x threshold of 500ms)
	state.LastTickTime = time.Now().Add(-200 * time.Millisecond)

	detector.scan(context.Background())

	if len(emitter.alerts) != 0 {
		t.Errorf("Expected 0 alerts within threshold, got %d", len(emitter.alerts))
	}
}

func TestDetector_ProportionalThreshold(t *testing.T) {
	detector, registry, emitter := setupTestDetector(t)

	// Liquid instrument: 10ms interval
	liquidKey := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "LIQUID",
	}
	liquidState := registry.GetOrCreate(liquidKey)
	for i := 0; i < 100; i++ {
		liquidState.StatsBySession[model.Open].Update(10.0, 0.5, 0.1, 1000)
		liquidState.AllSessionStats.Update(10.0, 0.5, 0.1, 1000)
	}
	liquidState.LastTickTime = time.Now().Add(-100 * time.Millisecond) // 10x threshold

	// Illiquid instrument: 1000ms interval
	illiquidKey := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "ILLIQUID",
	}
	illiquidState := registry.GetOrCreate(illiquidKey)
	for i := 0; i < 100; i++ {
		illiquidState.StatsBySession[model.Open].Update(1000.0, 0.5, 0.1, 1000)
		illiquidState.AllSessionStats.Update(1000.0, 0.5, 0.1, 1000)
	}
	illiquidState.LastTickTime = time.Now().Add(-10 * time.Second) // 10x threshold

	detector.scan(context.Background())

	// Both should alert at the same proportional deviation (10x)
	if len(emitter.alerts) != 2 {
		t.Fatalf("Expected 2 alerts (both at 10x threshold), got %d", len(emitter.alerts))
	}

	// Both should have MEDIUM latency level (> 10x but < 20x)
	for i, alert := range emitter.alerts {
		if alert.LatencyLevel != "MEDIUM" {
			t.Errorf("Alert %d: Expected MEDIUM latency level (10x threshold), got %s", i, alert.LatencyLevel)
		}
	}
}

func TestDetector_SessionFallback(t *testing.T) {
	detector, registry, emitter := setupTestDetector(t)

	// Create instrument with all-sessions stats but no session-specific stats
	key := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "SPARSE",
	}
	state := registry.GetOrCreate(key)

	// Warm up all-sessions but not Open session (< MinObservations)
	for i := 0; i < 10; i++ {
		state.StatsBySession[model.Open].Update(100.0, 0.5, 0.1, 1000)
	}
	for i := 0; i < 100; i++ {
		state.AllSessionStats.Update(100.0, 0.5, 0.1, 1000)
	}

	// Set silence
	state.LastTickTime = time.Now().Add(-1 * time.Second) // 10x threshold

	detector.scan(context.Background())

	// Should still alert using fallback stats
	if len(emitter.alerts) != 1 {
		t.Fatalf("Expected 1 alert using fallback stats, got %d", len(emitter.alerts))
	}
}

func TestDetermineLatencyLevel(t *testing.T) {
	tests := []struct {
		name              string
		elapsedMs         float64
		expectedMs        float64
		expectedLevel     string
	}{
		{"Low (6x)", 600, 100, "LOW"},
		{"Low (9x)", 900, 100, "LOW"},
		{"Medium (11x)", 1100, 100, "MEDIUM"},
		{"Medium (19x)", 1900, 100, "MEDIUM"},
		{"Severe (21x)", 2100, 100, "SEVERE"},
		{"Severe (100x)", 10000, 100, "SEVERE"},
		{"Illiquid Low (6x)", 30000, 5000, "LOW"},
		{"Illiquid Medium (15x)", 75000, 5000, "MEDIUM"},
		{"Illiquid Severe (25x)", 125000, 5000, "SEVERE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			level := model.DetermineLatencyLevel(tt.elapsedMs, tt.expectedMs)
			if level != tt.expectedLevel {
				t.Errorf("Expected %s, got %s (%.0fms / %.0fms = %.1fx)",
					tt.expectedLevel, level, tt.elapsedMs, tt.expectedMs, tt.elapsedMs/tt.expectedMs)
			}
		})
	}
}
