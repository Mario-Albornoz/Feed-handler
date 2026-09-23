package processing

import (
	"context"
	"encoding/json"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
	"math"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/validation"
)

// MockVectorEmitter mocks VectorEmitter for testing
type MockVectorEmitter struct {
	vectors    []*model.NormalizedVector
	shouldFail bool
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
		Timezone:       loc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
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
		ID:              "SPX",
		Exchange:        "NYSE",
		SecType:         "I", // Index, not equity
		LastTradedPrice: 4500.0,
		TotalVolume:     0,
		TradingTime:     time.Now(),
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
		ID:              "AAPL",
		Exchange:        "NYSE",
		SecType:         "E",
		LastTradedPrice: 150.0,
		TotalVolume:     1000,
		TradingTime:     time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC), // Monday 10 AM
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

	if state.PrevLastTradedPrice != 150.0 {
		t.Errorf("Expected PrevLastTradedPrice %f, got %f", 150.0, state.PrevLastTradedPrice)
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
		ID:              "AAPL",
		Exchange:        "NYSE",
		SecType:         "E",
		LastTradedPrice: 151.0,
		TotalVolume:     1100,
		TradingTime:     baseTime.Add(100 * time.Millisecond),
	}

	err = processor.ProcessRawTicks(context.Background(), tick2)
	if err != nil {
		t.Fatalf("Second ProcessRawTicks failed: %v", err)
	}

	// The intertick value is internal to the update, but we can verify that the rolling
	// stats were updated. The first tick has no predecessor and is not an observation,
	// so two ticks give one observation.
	key := model.InstrumentKey{
		Source:               "NYSE",
		InstrumentIdentifier: "AAPL",
	}
	state := registry.GetOrCreate(key)

	if state.AllSessionStats.ObservationCount != 1 {
		t.Errorf("Expected 1 observation, got %d", state.AllSessionStats.ObservationCount)
	}
}

func TestProcessRawTicks_PriceStepCalculation(t *testing.T) {
	processor, _, emitter := setupTestProcessor(t)

	baseTime := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)

	// First tick
	tick1 := &model.RawTick{
		ID:              "AAPL",
		Exchange:        "NYSE",
		SecType:         "E",
		LastTradedPrice: 150.0,
		TotalVolume:     1000,
		TradingTime:     baseTime,
	}

	err := processor.ProcessRawTicks(context.Background(), tick1)
	if err != nil {
		t.Fatalf("First tick failed: %v", err)
	}

	// Second tick with price movement
	tick2 := &model.RawTick{
		ID:              "AAPL",
		Exchange:        "NYSE",
		SecType:         "E",
		LastTradedPrice: 151.0,
		TotalVolume:     1100,
		TradingTime:     baseTime.Add(100 * time.Millisecond),
	}

	err = processor.ProcessRawTicks(context.Background(), tick2)
	if err != nil {
		t.Fatalf("Second tick failed: %v", err)
	}

	// PriceStep was calculated as abs(lastTradedPrice2 - lastTradedPrice1)
	// price1 = 150.0, price2 = 151.0, priceStep = 1.0
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
			Timezone:       loc,
			PreMarketStart: 4 * time.Hour,
			MarketOpen:     9*time.Hour + 30*time.Minute,
			MiddayStart:    12 * time.Hour,
			CloseStart:     15*time.Hour + 30*time.Minute,
			MarketClose:    16 * time.Hour,
			AfterHoursEnd:  20 * time.Hour,
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
		ID:              "AAPL",
		Exchange:        "NYSE",
		SecType:         "E",
		LastTradedPrice: 150.0,
		TotalVolume:     1000,
		TradingTime:     time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC),
	}

	// Should not error when producer is nil
	err := processor.ProcessRawTicks(context.Background(), tick)
	if err != nil {
		t.Errorf("Expected no error with nil producer, got %v", err)
	}
}

// ---- validator and silence observer wiring ----

type fakeObserver struct {
	ticks []time.Time
}

func (f *fakeObserver) ObserveTick(_ context.Context, _ model.InstrumentKey, _ *model.InstrumentState, tickTime time.Time) {
	f.ticks = append(f.ticks, tickTime)
}

type captureValidationEmitter struct {
	alerts []*model.ValidationAlert
}

func (c *captureValidationEmitter) WriteValidationAlert(_ context.Context, a *model.ValidationAlert) error {
	c.alerts = append(c.alerts, a)
	return nil
}

func processorWithOptions(t *testing.T, opts ...Option) (*FeedProcessor, *model.InstrumentRegistry, *MockVectorEmitter) {
	t.Helper()
	cfg := config.AggregatorConfig{
		Windows: config.WindowConfig{FastWindowTicks: 50, SlowWindowTicks: 200},
		CUSUM:   config.CUSUMConfig{Slack: 0.5},
	}
	loc, _ := time.LoadLocation("America/New_York")
	hours := &model.ExchangeHours{
		Timezone: loc, PreMarketStart: 4 * time.Hour, MarketOpen: 9*time.Hour + 30*time.Minute,
		MiddayStart: 12 * time.Hour, CloseStart: 15*time.Hour + 30*time.Minute,
		MarketClose: 16 * time.Hour, AfterHoursEnd: 20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}
	resolver := model.NewSessionResolverWithDefault(hours, nil)
	registry := model.NewInstrumentRegistry(cfg.Windows.FastWindowTicks, cfg.Windows.SlowWindowTicks, cfg.CUSUM.Slack)
	emitter := &MockVectorEmitter{}
	return NewFeedProcessor(cfg, resolver, registry, emitter, opts...), registry, emitter
}

func equityTick(at time.Time, isin string) *model.RawTick {
	return &model.RawTick{
		ID: "SAP.ETR", Exchange: "ETR", SecType: "E", ISIN: isin,
		LastTradedPrice: 100.0, TradingTime: at,
	}
}

func TestValidatorQuarantinesMalformedTicks(t *testing.T) {
	alerts := &captureValidationEmitter{}
	observer := &fakeObserver{}
	processor, registry, emitter := processorWithOptions(t,
		WithValidator(validation.New(1000, alerts)),
		WithSilenceObserver(observer),
	)
	ctx := context.Background()
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	processor.ProcessRawTicks(ctx, equityTick(base, ""))
	processor.ProcessRawTicks(ctx, equityTick(base.Add(time.Second), ""))

	// A rewound timestamp and a junk ISIN: both rejected, neither may leave a trace.
	processor.ProcessRawTicks(ctx, equityTick(base.Add(-5*time.Minute), ""))
	processor.ProcessRawTicks(ctx, equityTick(base.Add(2*time.Second), "XXX"))

	processor.ProcessRawTicks(ctx, equityTick(base.Add(3*time.Second), ""))

	if len(emitter.vectors) != 3 {
		t.Errorf("expected 3 vectors (2 rejected ticks skipped), got %d", len(emitter.vectors))
	}
	if len(alerts.alerts) != 2 {
		t.Fatalf("expected 2 validation alerts, got %d", len(alerts.alerts))
	}
	if alerts.alerts[0].AlertType != model.AlertTimestampInversion || alerts.alerts[1].AlertType != model.AlertMalformedISIN {
		t.Errorf("unexpected alert types: %s, %s", alerts.alerts[0].AlertType, alerts.alerts[1].AlertType)
	}

	// Rejected ticks still show the feed is alive, so the silence detector sees all 5
	// messages, but they must not rewind instrument state (checked below).
	if len(observer.ticks) != 5 {
		t.Errorf("observer should see all 5 messages (3 accepted, 2 rejected), got %d", len(observer.ticks))
	}
	state := registry.GetOrCreate(model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"})
	if want := base.Add(3 * time.Second); !state.LastTickTime.Equal(want) {
		t.Errorf("LastTickTime: got %v, want %v (a rejected tick must not change it)", state.LastTickTime, want)
	}
}

func TestValidatorIgnoresNormalBackwardNoise(t *testing.T) {
	alerts := &captureValidationEmitter{}
	processor, _, emitter := processorWithOptions(t, WithValidator(validation.New(1000, alerts)))
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	processor.ProcessRawTicks(context.Background(), equityTick(base, ""))
	processor.ProcessRawTicks(context.Background(), equityTick(base.Add(-400*time.Millisecond), ""))

	if len(alerts.alerts) != 0 || len(emitter.vectors) != 2 {
		t.Errorf("sub-second backward step must pass: alerts=%d vectors=%d", len(alerts.alerts), len(emitter.vectors))
	}
}

func TestSilenceObserverSeesTickBeforeStateUpdate(t *testing.T) {
	var lastSeenByObserver []time.Time
	registryRef := (*model.InstrumentRegistry)(nil)
	observer := observerFunc(func(key model.InstrumentKey, tickTime time.Time) {
		lastSeenByObserver = append(lastSeenByObserver, registryRef.GetOrCreate(key).LastTickTime)
	})
	processor, registry, _ := processorWithOptions(t, WithSilenceObserver(observer))
	registryRef = registry
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	processor.ProcessRawTicks(context.Background(), equityTick(base, ""))
	processor.ProcessRawTicks(context.Background(), equityTick(base.Add(10*time.Second), ""))

	if len(lastSeenByObserver) != 2 || !lastSeenByObserver[0].IsZero() || !lastSeenByObserver[1].Equal(base) {
		t.Errorf("observer must see the previous tick time, not the current one: %v", lastSeenByObserver)
	}
}

type observerFunc func(key model.InstrumentKey, tickTime time.Time)

func (f observerFunc) ObserveTick(_ context.Context, key model.InstrumentKey, _ *model.InstrumentState, tickTime time.Time) {
	f(key, tickTime)
}

func TestFirstTickIsNotAnObservation(t *testing.T) {
	processor, registry, _ := processorWithOptions(t)
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	processor.ProcessRawTicks(context.Background(), equityTick(base, ""))
	state := registry.GetOrCreate(model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"})
	if state.AllSessionStats.ObservationCount != 0 {
		t.Errorf("the first tick has no predecessor and must not be counted, got %d observations",
			state.AllSessionStats.ObservationCount)
	}

	processor.ProcessRawTicks(context.Background(), equityTick(base.Add(time.Second), ""))
	if state.AllSessionStats.ObservationCount != 1 {
		t.Errorf("the second tick is the first observation, got %d", state.AllSessionStats.ObservationCount)
	}
}

// ---- last traded price is carried forward through quote-only messages ----

func msgAt(at time.Time, price float64) *model.RawTick {
	return &model.RawTick{ID: "SAP.ETR", Exchange: "ETR", SecType: "E", LastTradedPrice: price, TradingTime: at}
}

func TestQuoteMessagesDoNotResetThePrice(t *testing.T) {
	processor, registry, emitter := processorWithOptions(t)
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)
	key := model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"}

	// trade 100, two quote updates (empty last price), trade 101
	prices := []float64{100, 0, 0, 101}
	for i, p := range prices {
		processor.ProcessRawTicks(context.Background(), msgAt(base.Add(time.Duration(i)*time.Second), p))
	}

	state := registry.GetOrCreate(key)
	if state.PrevLastTradedPrice != 101 {
		t.Errorf("last traded price should be the last trade (101), got %v", state.PrevLastTradedPrice)
	}
	// only the 100 -> 101 change is a price observation; the quotes are not, and
	// neither is the first trade (it has no previous trade)
	if state.AllSessionStats.PriceObservationCount != 1 {
		t.Errorf("price observations: got %d, want 1", state.AllSessionStats.PriceObservationCount)
	}
	// timing is observed on every message but the first
	if state.AllSessionStats.ObservationCount != 3 {
		t.Errorf("timing observations: got %d, want 3", state.AllSessionStats.ObservationCount)
	}

	wantTrade := []int{1, 0, 0, 1}
	for i, v := range emitter.vectors {
		if v.HasTrade != wantTrade[i] {
			t.Errorf("vector %d: has_trade=%d, want %d", i, v.HasTrade, wantTrade[i])
		}
	}
	for _, i := range []int{0, 1, 2} {
		v := emitter.vectors[i]
		if v.ZPriceStepFast != 0 || v.ZPriceStepSlow != 0 {
			t.Errorf("vector %d has no price step, its price z-scores must be 0, got %v/%v", i, v.ZPriceStepFast, v.ZPriceStepSlow)
		}
	}
}

func TestPriceStepIsBetweenConsecutiveTrades(t *testing.T) {
	processor, registry, _ := processorWithOptions(t)
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)
	key := model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"}

	// many quote updates between two trades must not look like a big jump to 0 and back
	processor.ProcessRawTicks(context.Background(), msgAt(base, 100))
	for i := 1; i <= 20; i++ {
		processor.ProcessRawTicks(context.Background(), msgAt(base.Add(time.Duration(i)*time.Second), 0))
	}
	processor.ProcessRawTicks(context.Background(), msgAt(base.Add(30*time.Second), 100.5))

	state := registry.GetOrCreate(key)
	if got := state.AllSessionStats.SlowMeanPriceStep; got < 0.499 || got > 0.501 {
		t.Errorf("the only price step is 0.5, mean price step is %v", got)
	}
}

func TestPriceCusumIsCarriedOnQuoteMessages(t *testing.T) {
	processor, registry, emitter := processorWithOptions(t)
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	price := 100.0
	// trades with a growing step so the price CUSUM builds up, then quote-only messages
	for i := 0; i < 80; i++ {
		price += 0.01 + 0.01*float64(i)
		processor.ProcessRawTicks(context.Background(), msgAt(base.Add(time.Duration(i)*time.Second), price))
	}
	afterTrades := emitter.vectors[len(emitter.vectors)-1].CusumPriceStep
	processor.ProcessRawTicks(context.Background(), msgAt(base.Add(100*time.Second), 0))
	afterQuote := emitter.vectors[len(emitter.vectors)-1].CusumPriceStep

	if afterTrades == 0 {
		t.Fatal("test setup: the price CUSUM should have built up")
	}
	if afterQuote != afterTrades {
		t.Errorf("a quote message must not change the price CUSUM: %v -> %v", afterTrades, afterQuote)
	}
	_ = registry
}

// ---- feature clock (whole-second update time), overnight gap, price warm-up, wire format ----

func msgWithClocks(update, trading time.Time, price float64) *model.RawTick {
	return &model.RawTick{ID: "SAP.ETR", Exchange: "ETR", SecType: "E", LastTradedPrice: price,
		Time: update, TradingTime: trading}
}

func TestTimingRunsOnTheUpdateTimeNotTheJitteryTradingTime(t *testing.T) {
	processor, registry, emitter := processorWithOptions(t)
	key := model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"}
	sec := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	// same second; TradingTime jumps .172, then back to .000, then .050: 30% of real steps
	// look like this. On the update time these are all the same instant.
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec, sec.Add(172*time.Millisecond), 0))
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec, sec, 0))
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec, sec.Add(50*time.Millisecond), 0))
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec.Add(time.Second), sec.Add(time.Second), 0))

	state := registry.GetOrCreate(key)
	if state.Gaps.N != 3 {
		t.Fatalf("expected 3 gap observations, got %d", state.Gaps.N)
	}
	// gaps on the update clock: 0, 0, 1000 ms; a backward step would have entered as a
	// negative value and pulled the mean below the true 333 ms
	if got := state.AllSessionStats.SlowMeanIntertick; got < 333 || got > 334 {
		t.Errorf("mean gap on the update clock should be 333.3 ms, got %.2f", got)
	}
	if !state.LastTickTime.Equal(sec.Add(time.Second)) {
		t.Errorf("LastTickTime is on the update clock, got %v", state.LastTickTime)
	}
	// the vector keeps the millisecond TradingTime as the message's identity
	if got := emitter.vectors[0].Timestamp; !got.Equal(sec.Add(172 * time.Millisecond)) {
		t.Errorf("vector timestamp should be TradingTime, got %v", got)
	}
}

// A rewound TradingTime (phase 4) is caught by the validator against the previous
// TradingTime, and never disturbs the update-time timeline the features run on.
func TestRewoundTradingTimeIsRejectedAndDoesNotTouchTheFeatureClock(t *testing.T) {
	alerts := &captureValidationEmitter{}
	processor, registry, emitter := processorWithOptions(t, WithValidator(validation.New(1000, alerts)))
	key := model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"}
	sec := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec, sec.Add(300*time.Millisecond), 0))
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec.Add(time.Second), sec.Add(time.Second).Add(-5*time.Minute), 0))
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec.Add(2*time.Second), sec.Add(2*time.Second), 0))

	if len(alerts.alerts) != 1 || alerts.alerts[0].AlertType != model.AlertTimestampInversion {
		t.Fatalf("expected one timestamp inversion alert, got %+v", alerts.alerts)
	}
	if len(emitter.vectors) != 2 {
		t.Errorf("the rejected message must not produce a vector, got %d vectors", len(emitter.vectors))
	}
	state := registry.GetOrCreate(key)
	if !state.LastTickTime.Equal(sec.Add(2 * time.Second)) {
		t.Errorf("LastTickTime: got %v", state.LastTickTime)
	}
}

func TestOvernightGapIsNotAnObservation(t *testing.T) {
	processor, registry, _ := processorWithOptions(t)
	key := model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"}
	evening := time.Date(2021, 11, 10, 16, 0, 0, 0, time.UTC)

	processor.ProcessRawTicks(context.Background(), msgWithClocks(evening, evening, 0))
	processor.ProcessRawTicks(context.Background(), msgWithClocks(evening.Add(time.Second), evening.Add(time.Second), 0))
	state := registry.GetOrCreate(key)
	if state.Gaps.N != 1 {
		t.Fatalf("setup: expected 1 observation, got %d", state.Gaps.N)
	}

	morning := time.Date(2021, 11, 11, 9, 30, 0, 0, time.UTC)
	processor.ProcessRawTicks(context.Background(), msgWithClocks(morning, morning, 0))
	if state.Gaps.N != 1 || state.AllSessionStats.ObservationCount != 1 {
		t.Errorf("the 17-hour overnight gap must not be observed: gaps=%d observations=%d",
			state.Gaps.N, state.AllSessionStats.ObservationCount)
	}
	if !state.LastTickTime.Equal(morning) {
		t.Errorf("the new day's first message still becomes the reference, got %v", state.LastTickTime)
	}

	processor.ProcessRawTicks(context.Background(), msgWithClocks(morning.Add(2*time.Second), morning.Add(2*time.Second), 0))
	if state.Gaps.N != 2 {
		t.Errorf("gaps within the new day are observed again, got %d", state.Gaps.N)
	}
}

// The first message of a new day has no interval (a placeholder 0). Against a steady
// baseline whose variance has decayed towards zero it used to score about -1e9 (the
// market-open spikes of run thesis_20260923_003839); it must get neutral z-scores.
func TestNewDayFirstMessageHasNeutralTimingZScores(t *testing.T) {
	processor, registry, emitter := processorWithOptions(t)
	key := model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"}
	at := time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)

	// one 2 s interval, then a long run of identical 1 s intervals: the variance decays
	// towards zero without reaching it
	processor.ProcessRawTicks(context.Background(), msgWithClocks(at, at, 0))
	at = at.Add(2 * time.Second)
	processor.ProcessRawTicks(context.Background(), msgWithClocks(at, at, 0))
	for i := 0; i < 500; i++ {
		at = at.Add(time.Second)
		processor.ProcessRawTicks(context.Background(), msgWithClocks(at, at, 0))
	}
	state := registry.GetOrCreate(key)
	if std := math.Sqrt(state.AllSessionStats.FastVarIntertick); std <= 1e-10 || std > 1 {
		t.Fatalf("setup: the fast baseline should be steady (std under 1 ms against a 1 s mean) but nonzero, std %g", std)
	}

	morning := time.Date(2021, 11, 11, 9, 30, 0, 0, time.UTC)
	processor.ProcessRawTicks(context.Background(), msgWithClocks(morning, morning, 0))
	v := emitter.vectors[len(emitter.vectors)-1]
	if v.ZIntertickFast != 0 || v.ZIntertickSlow != 0 {
		t.Errorf("the new day's first message has no interval to score: z fast %g, z slow %g",
			v.ZIntertickFast, v.ZIntertickSlow)
	}

	// the next message of the day has a real interval (2 s against a 1 s baseline)
	processor.ProcessRawTicks(context.Background(), msgWithClocks(morning.Add(2*time.Second), morning.Add(2*time.Second), 0))
	if v := emitter.vectors[len(emitter.vectors)-1]; v.ZIntertickFast <= 0 {
		t.Errorf("a real interval within the day is scored again, z fast %g", v.ZIntertickFast)
	}
}

func TestPriceWarmupRequirementIsConfigurable(t *testing.T) {
	cfg := config.AggregatorConfig{
		Windows: config.WindowConfig{FastWindowTicks: 50, SlowWindowTicks: 200},
		CUSUM:   config.CUSUMConfig{Slack: 0.5},
	}
	loc, _ := time.LoadLocation("UTC")
	hours := &model.ExchangeHours{Timezone: loc, PreMarketStart: 0, MarketOpen: time.Hour, MiddayStart: 2 * time.Hour,
		CloseStart: 3 * time.Hour, MarketClose: 23 * time.Hour, AfterHoursEnd: 24 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true}}
	registry := model.NewInstrumentRegistry(50, 200, 0.5)
	registry.SetMinPriceObservations(5)
	processor := NewFeedProcessor(cfg, model.NewSessionResolverWithDefault(hours, nil), registry, &MockVectorEmitter{})

	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ { // 6 trades = 5 price steps
		processor.ProcessRawTicks(context.Background(), msgAt(base.Add(time.Duration(i)*time.Second), 100+float64(i)))
	}
	state := registry.GetOrCreate(model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"})
	if state.AllSessionStats.PriceObservationCount < state.AllSessionStats.MinPriceObservations {
		t.Errorf("5 price steps should warm the price statistics when the requirement is 5 (count %d, min %d)",
			state.AllSessionStats.PriceObservationCount, state.AllSessionStats.MinPriceObservations)
	}
	if state.AllSessionStats.IsWarm() {
		t.Error("the timing statistics keep their own, higher requirement")
	}
}

// The path a real message takes: the simulator's JSON, decoded by the handler, through
// the pipeline. Before the wire-format fix the price was 0 here and nothing downstream
// ever saw a trade.
func TestSimulatorMessagesProduceTradesAndPriceSteps(t *testing.T) {
	processor, registry, emitter := processorWithOptions(t)
	messages := []string{
		`{"ID":"SAP.ETR","Exchange":"ETR","SecType":"E","Bid":0,"Ask":0,"TotalVolume":10,"Last":100,"TradingTime":"2021-11-11T10:00:00.400Z","Date":"2021-11-11T00:00:00Z","Time":"2021-11-11T10:00:00Z"}`,
		`{"ID":"SAP.ETR","Exchange":"ETR","SecType":"E","Bid":99,"Ask":0,"TotalVolume":0,"Last":0,"TradingTime":"2021-11-11T10:00:01Z","Date":"2021-11-11T00:00:00Z","Time":"2021-11-11T10:00:01Z"}`,
		`{"ID":"SAP.ETR","Exchange":"ETR","SecType":"E","Bid":0,"Ask":0,"TotalVolume":20,"Last":101.5,"TradingTime":"2021-11-11T10:00:02.250Z","Date":"2021-11-11T00:00:00Z","Time":"2021-11-11T10:00:02Z"}`,
	}
	for _, m := range messages {
		var tick model.RawTick
		if err := json.Unmarshal([]byte(m), &tick); err != nil {
			t.Fatal(err)
		}
		processor.ProcessRawTicks(context.Background(), &tick)
	}

	state := registry.GetOrCreate(model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"})
	if state.PrevLastTradedPrice != 101.5 {
		t.Errorf("last traded price: got %v, want 101.5", state.PrevLastTradedPrice)
	}
	if state.AllSessionStats.PriceObservationCount != 1 {
		t.Errorf("one trade-to-trade step expected, got %d", state.AllSessionStats.PriceObservationCount)
	}
	if got := state.AllSessionStats.SlowMeanPriceStep; got != 1.5 {
		t.Errorf("price step: got %v, want 1.5", got)
	}
	if hasTrade := []int{emitter.vectors[0].HasTrade, emitter.vectors[1].HasTrade, emitter.vectors[2].HasTrade}; hasTrade[0] != 1 || hasTrade[1] != 0 || hasTrade[2] != 1 {
		t.Errorf("has_trade flags: got %v, want [1 0 1]", hasTrade)
	}
}

type recordingObserver struct{ seen []time.Time }

func (r *recordingObserver) ObserveTick(_ context.Context, _ model.InstrumentKey, _ *model.InstrumentState, tickTime time.Time) {
	r.seen = append(r.seen, tickTime)
}

// A quarantined message is still a message: the feed is alive. It must be shown to the
// silence detector and move the last-seen time, without becoming a statistics observation.
func TestQuarantinedMessageStillProvesTheFeedIsAlive(t *testing.T) {
	alerts := &captureValidationEmitter{}
	observer := &recordingObserver{}
	processor, registry, _ := processorWithOptions(t, WithValidator(validation.New(1000, alerts)), WithSilenceObserver(observer))
	key := model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"}
	sec := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec, sec, 0))
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec.Add(time.Second), sec.Add(time.Second), 0))
	state := registry.GetOrCreate(key)
	gapsBefore := state.Gaps.N
	lastTradingBefore := state.LastTradingTime

	// second 2: TradingTime rewound five minutes, so rejected; its update time is fine
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec.Add(2*time.Second), sec.Add(2*time.Second).Add(-5*time.Minute), 0))

	if len(alerts.alerts) != 1 {
		t.Fatalf("setup: expected the rewound message to be rejected, got %d alerts", len(alerts.alerts))
	}
	if len(observer.seen) != 3 || !observer.seen[2].Equal(sec.Add(2*time.Second)) {
		t.Errorf("the silence detector must see the rejected message too: %v", observer.seen)
	}
	if !state.LastTickTime.Equal(sec.Add(2 * time.Second)) {
		t.Errorf("last-seen should move to the rejected message's update time, got %v", state.LastTickTime)
	}
	if state.Gaps.N != gapsBefore {
		t.Errorf("a rejected message must not become a statistics observation (%d -> %d)", gapsBefore, state.Gaps.N)
	}
	if !state.LastTradingTime.Equal(lastTradingBefore) {
		t.Errorf("the validator's reference time must stay on the last accepted message, got %v", state.LastTradingTime)
	}

	// the next accepted message sees a 1 s gap, not 2 s
	processor.ProcessRawTicks(context.Background(), msgWithClocks(sec.Add(3*time.Second), sec.Add(3*time.Second), 0))
	if got := state.AllSessionStats.SlowMeanIntertick; got != 1000 {
		t.Errorf("gaps observed should all be 1000 ms, mean is %v", got)
	}
}

func TestVectorCarriesTheMessageSequenceNumber(t *testing.T) {
	processor, _, emitter := processorWithOptions(t)
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)
	for i, seq := range []uint64{41, 42, 43} {
		m := msgAt(base.Add(time.Duration(i)*time.Second), 100)
		m.Seq = seq
		processor.ProcessRawTicks(context.Background(), m)
	}
	for i, want := range []uint64{41, 42, 43} {
		if emitter.vectors[i].Seq != want {
			t.Errorf("vector %d: seq %d, want %d", i, emitter.vectors[i].Seq, want)
		}
	}
}

// TestVectorCarriesTheRawMeasurements checks the un-normalized fields an ablation needs:
// the inter-tick interval, the price step and the reference price, with flags that tell a
// real 0 from a placeholder.
func TestVectorCarriesTheRawMeasurements(t *testing.T) {
	processor, _, emitter := processorWithOptions(t)
	base := time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

	processor.ProcessRawTicks(context.Background(), msgAt(base, 100))                  // first message, a trade
	processor.ProcessRawTicks(context.Background(), msgAt(base.Add(3*time.Second), 0)) // a quote, 3 s later
	processor.ProcessRawTicks(context.Background(), msgAt(base.Add(5*time.Second), 100.5))

	if len(emitter.vectors) != 3 {
		t.Fatalf("want 3 vectors, got %d", len(emitter.vectors))
	}
	first, quote, trade := emitter.vectors[0], emitter.vectors[1], emitter.vectors[2]

	if first.HasIntertick != 0 || first.IntertickMs != 0 || first.HasPriceStep != 0 || first.RefPrice != 0 {
		t.Errorf("first message has no predecessor: got %+v", first)
	}
	if quote.HasIntertick != 1 || quote.IntertickMs != 3000 {
		t.Errorf("quote: want a 3000 ms interval, got has=%d ms=%v", quote.HasIntertick, quote.IntertickMs)
	}
	if quote.HasTrade != 0 || quote.HasPriceStep != 0 || quote.PriceStep != 0 || quote.RefPrice != 100 {
		t.Errorf("quote: no price step, reference is the last trade (100): got %+v", quote)
	}
	if trade.IntertickMs != 2000 || trade.HasPriceStep != 1 || math.Abs(trade.PriceStep-0.5) > 1e-9 || trade.RefPrice != 100 {
		t.Errorf("trade: want interval 2000 ms, step 0.5 from 100: got %+v", trade)
	}
}

// TestCusumIsResetOnTheFirstMessageOfANewDay: evidence accumulated on one day must not
// carry into the next, unless the reset is switched off.
func TestCusumIsResetOnTheFirstMessageOfANewDay(t *testing.T) {
	run := func(reset bool) (endOfDay, nextMorning float64) {
		processor, registry, emitter := processorWithOptions(t)
		key := model.InstrumentKey{Source: "ETR", InstrumentIdentifier: "SAP.ETR"}
		registry.GetOrCreate(key) // created with the default limits
		for _, s := range append([]*stats.RollingStats{registry.GetOrCreate(key).AllSessionStats}, statsOf(registry.GetOrCreate(key))...) {
			s.Limits.ResetCusumDaily = reset
		}
		day1 := time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)
		price := 100.0
		for i := 0; i < 80; i++ { // growing steps build up the price CUSUM
			price += 0.01 + 0.01*float64(i)
			processor.ProcessRawTicks(context.Background(), msgAt(day1.Add(time.Duration(i)*time.Second), price))
		}
		endOfDay = emitter.vectors[len(emitter.vectors)-1].CusumPriceStep
		day2 := time.Date(2021, 11, 11, 9, 30, 0, 0, time.UTC)
		processor.ProcessRawTicks(context.Background(), msgAt(day2, price))
		return endOfDay, emitter.vectors[len(emitter.vectors)-1].CusumPriceStep
	}
	end, morning := run(true)
	if end <= 0 {
		t.Fatalf("test setup: the price CUSUM should have built up on day 1, got %v", end)
	}
	if morning != 0 {
		t.Errorf("with the daily reset the CUSUM must be 0 on the next morning, got %v", morning)
	}
	end, morning = run(false)
	if morning != end {
		t.Errorf("without the reset the CUSUM carries over: end of day %v, next morning %v", end, morning)
	}
}

func statsOf(s *model.InstrumentState) []*stats.RollingStats {
	out := []*stats.RollingStats{}
	for _, r := range s.StatsBySession {
		out = append(out, r)
	}
	return out
}
