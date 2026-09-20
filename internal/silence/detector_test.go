package silence

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

// MockAlertEmitter for testing
type MockAlertEmitter struct {
	mu     sync.Mutex
	alerts []*model.SilenceAlert
}

func (m *MockAlertEmitter) WriteAlert(ctx context.Context, alert *model.SilenceAlert) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alerts = append(m.alerts, alert)
	return nil
}

func (m *MockAlertEmitter) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.alerts)
}

const testExchange = "ETR"

var t0 = time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)

var testConfig = Config{Quantile: 0.999, Multiplier: 1.0, MinObservations: 50, MinThresholdMs: 1000}

func setupTestDetector(t *testing.T) (*Detector, *model.InstrumentRegistry, *model.EventClock, *MockAlertEmitter) {
	t.Helper()

	registry := model.NewInstrumentRegistry(50, 14400, 0.5)
	clock := model.NewEventClock()
	emitter := &MockAlertEmitter{}
	detector := NewDetector(registry, clock, emitter, testConfig, 5*time.Second)
	return detector, registry, clock, emitter
}

// warmInstrument registers an instrument that normally ticks every second: of 2,000
// observed gaps 1,990 are 1 s, 9 are 4 s and one is 30 s, so the 99.9th percentile is
// about 4-5 s. Its last message is at lastTick.
func warmInstrument(registry *model.InstrumentRegistry, id string, lastTick time.Time) (model.InstrumentKey, *model.InstrumentState) {
	return warmInstrumentWithGaps(registry, id, lastTick, 1000, 4000, 30000)
}

func warmInstrumentWithGaps(registry *model.InstrumentRegistry, id string, lastTick time.Time, typical, rare, rarest float64) (model.InstrumentKey, *model.InstrumentState) {
	key := model.InstrumentKey{Source: testExchange, InstrumentIdentifier: id}
	state := registry.GetOrCreate(key)
	for i := 0; i < 1990; i++ {
		state.Gaps.Observe(typical)
	}
	for i := 0; i < 9; i++ {
		state.Gaps.Observe(rare)
	}
	state.Gaps.Observe(rarest)
	state.LastTickTime = lastTick
	return key, state
}

func TestDetector_NoAlertBeforeMinObservations(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)

	key := model.InstrumentKey{Source: testExchange, InstrumentIdentifier: "COLD"}
	state := registry.GetOrCreate(key)
	for i := 0; i < 10; i++ {
		state.Gaps.Observe(1000)
	}
	state.LastTickTime = t0
	clock.Advance(testExchange, t0.Add(time.Hour))

	detector.scan(context.Background(), model.TriggerScan)

	if emitter.count() != 0 {
		t.Errorf("Expected 0 alerts before the minimum number of observations, got %d", emitter.count())
	}
}

func TestDetector_ThresholdIsTheInstrumentsOwnQuantile(t *testing.T) {
	_, registry, _, _ := setupTestDetector(t)
	_, state := warmInstrument(registry, "AAPL", t0)

	q := state.Gaps.Quantile(0.999)
	if q < 4000 || q > 4800 {
		t.Errorf("99.9th percentile of the warm-up gaps should be just above 4 s, got %.0f ms", q)
	}
}

func TestDetector_ScanAlertsOnceForOngoingSilence(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	_, state := warmInstrument(registry, "AAPL", t0)
	threshold := state.Gaps.Quantile(0.999)

	clock.Advance(testExchange, t0.Add(60*time.Second))
	detector.scan(context.Background(), model.TriggerScan)

	if emitter.count() != 1 {
		t.Fatalf("Expected 1 alert, got %d", emitter.count())
	}
	alert := emitter.alerts[0]
	if alert.Exchange != testExchange || alert.Instrument != "AAPL" || alert.AlertType != "SILENCE" {
		t.Errorf("unexpected alert identity: %+v", alert)
	}
	if !alert.LastSeen.Equal(t0) {
		t.Errorf("LastSeen: got %v, want %v", alert.LastSeen, t0)
	}
	// Detection time is when the silence first exceeded the threshold, not when the
	// scan happened to run.
	if want := t0.Add(time.Duration(threshold * float64(time.Millisecond))); !alert.DetectedAt.Equal(want) {
		t.Errorf("DetectedAt: got %v, want %v", alert.DetectedAt, want)
	}
	if want := t0.Add(60 * time.Second); !alert.ObservedAt.Equal(want) {
		t.Errorf("ObservedAt: got %v, want %v", alert.ObservedAt, want)
	}
	if alert.ThresholdMs != threshold || alert.ElapsedMs != 60000 {
		t.Errorf("threshold/elapsed: got %.0f/%d, want %.0f/60000", alert.ThresholdMs, alert.ElapsedMs, threshold)
	}
	if alert.Trigger != model.TriggerScan {
		t.Errorf("Trigger: got %q, want scan", alert.Trigger)
	}
	if alert.LatencyLevel != "SEVERE" {
		t.Errorf("Expected SEVERE (60 s is over 10x a ~4.5 s threshold), got %s", alert.LatencyLevel)
	}

	// The same silence must not alert again, however often it is scanned.
	clock.Advance(testExchange, t0.Add(120*time.Second))
	detector.scan(context.Background(), model.TriggerScan)
	if emitter.count() != 1 {
		t.Errorf("Expected the silence to be reported once, got %d alerts", emitter.count())
	}
}

func TestDetector_NoAlertWithinThreshold(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	warmInstrument(registry, "AAPL", t0)

	// a 3 s gap is within what this instrument does
	clock.Advance(testExchange, t0.Add(3*time.Second))
	detector.scan(context.Background(), model.TriggerScan)

	if emitter.count() != 0 {
		t.Errorf("Expected 0 alerts within the instrument's normal gaps, got %d", emitter.count())
	}
}

func TestDetector_ResumeDetectsSilenceTheScanMissed(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	key, state := warmInstrument(registry, "AAPL", t0)
	threshold := state.Gaps.Quantile(0.999)

	// The instrument ticks again 60 s later; no scan ran in between (fast replay).
	resume := t0.Add(60 * time.Second)
	detector.ObserveTick(context.Background(), key, state, resume)

	if emitter.count() != 1 {
		t.Fatalf("Expected 1 alert from the resume path, got %d", emitter.count())
	}
	alert := emitter.alerts[0]
	if alert.Trigger != model.TriggerResume {
		t.Errorf("Trigger: got %q, want resume", alert.Trigger)
	}
	// Same detection time as the scan path would have reported.
	if want := t0.Add(time.Duration(threshold * float64(time.Millisecond))); !alert.DetectedAt.Equal(want) {
		t.Errorf("DetectedAt: got %v, want %v", alert.DetectedAt, want)
	}
	if !alert.ObservedAt.Equal(resume) {
		t.Errorf("ObservedAt: got %v, want %v", alert.ObservedAt, resume)
	}
	if now, _ := clock.Now(testExchange); !now.Equal(resume) {
		t.Errorf("ObserveTick should advance the event clock, got %v", now)
	}

	// The pipeline then records the tick; the silence is over and nothing more fires.
	state.Lock()
	state.LastTickTime = resume
	state.Unlock()
	detector.scan(context.Background(), model.TriggerScan)
	if emitter.count() != 1 {
		t.Errorf("Expected no further alerts, got %d", emitter.count())
	}
}

func TestDetector_ResumeAfterScanDoesNotAlertTwice(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	key, state := warmInstrument(registry, "AAPL", t0)

	clock.Advance(testExchange, t0.Add(30*time.Second))
	detector.scan(context.Background(), model.TriggerScan)
	detector.ObserveTick(context.Background(), key, state, t0.Add(60*time.Second))

	if emitter.count() != 1 {
		t.Errorf("Expected the silence to be reported once, got %d alerts", emitter.count())
	}
}

func TestDetector_NormalTickIsNotSilence(t *testing.T) {
	detector, registry, _, emitter := setupTestDetector(t)
	key, state := warmInstrument(registry, "AAPL", t0)

	detector.ObserveTick(context.Background(), key, state, t0.Add(time.Second))
	detector.ObserveTick(context.Background(), key, state, t0.Add(-time.Second)) // earlier: not a gap

	if emitter.count() != 0 {
		t.Errorf("Expected 0 alerts, got %d", emitter.count())
	}
}

// The point of using each instrument's own gaps: the same 20 s of quiet is a silence for
// an instrument that ticks every second and normal for one that ticks every 30 s.
func TestDetector_ThresholdAdaptsToEachInstrument(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	warmInstrument(registry, "BUSY", t0)
	warmInstrumentWithGaps(registry, "SLOW", t0, 30000, 90000, 200000)

	clock.Advance(testExchange, t0.Add(20*time.Second))
	detector.scan(context.Background(), model.TriggerScan)

	if emitter.count() != 1 || emitter.alerts[0].Instrument != "BUSY" {
		t.Fatalf("Expected only BUSY to alert after 20 s, got %d alerts: %+v", emitter.count(), emitter.alerts)
	}
}

// Timestamps have whole-second resolution, so a threshold below that cannot be told from
// rounding: an instrument whose gaps are almost all 0 must not alert on a 500 ms gap.
func TestDetector_ThresholdHasAClockResolutionFloor(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	key := model.InstrumentKey{Source: testExchange, InstrumentIdentifier: "BURSTY"}
	state := registry.GetOrCreate(key)
	for i := 0; i < 5000; i++ {
		state.Gaps.Observe(0)
	}
	state.LastTickTime = t0

	clock.Advance(testExchange, t0.Add(500*time.Millisecond))
	detector.scan(context.Background(), model.TriggerScan)
	if emitter.count() != 0 {
		t.Fatalf("a 500 ms gap is below the 1 s clock resolution, got %d alerts", emitter.count())
	}

	clock.Advance(testExchange, t0.Add(5*time.Second))
	detector.scan(context.Background(), model.TriggerScan)
	if emitter.count() != 1 || emitter.alerts[0].ThresholdMs != 1000 {
		t.Errorf("a 5 s gap should alert at the 1000 ms floor, got %+v", emitter.alerts)
	}
}

// The gap from one day's last message to the next day's first is the overnight closure.
func TestDetector_OvernightGapIsNotASilence(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	key, state := warmInstrument(registry, "AAPL", time.Date(2021, 11, 10, 16, 0, 0, 0, time.UTC))

	nextMorning := time.Date(2021, 11, 11, 9, 30, 0, 0, time.UTC)
	detector.ObserveTick(context.Background(), key, state, nextMorning)
	clock.Advance(testExchange, nextMorning.Add(time.Hour))
	detector.scan(context.Background(), model.TriggerScan)

	if emitter.count() != 0 {
		t.Errorf("Expected no alert across the overnight closure, got %d", emitter.count())
	}
}

func TestDetector_ExchangeWithoutClockIsSkipped(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	warmInstrument(registry, "AAPL", t0)

	// Only another exchange has been seen, so this exchange has no "now" yet.
	clock.Advance("FR", t0.Add(time.Hour))
	detector.scan(context.Background(), model.TriggerScan)

	if emitter.count() != 0 {
		t.Errorf("Expected 0 alerts, got %d", emitter.count())
	}
}

func TestDetector_RunFlushesOpenSilencesOnShutdown(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	detector.checkInterval = time.Hour // no periodic scan during the test
	warmInstrument(registry, "AAPL", t0)
	clock.Advance(testExchange, t0.Add(time.Minute))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- detector.Run(ctx) }()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("detector did not stop")
	}

	if emitter.count() != 1 {
		t.Fatalf("Expected the final scan to report the open silence, got %d alerts", emitter.count())
	}
	if emitter.alerts[0].Trigger != model.TriggerFlush {
		t.Errorf("Trigger: got %q, want flush", emitter.alerts[0].Trigger)
	}
}

// The consumer goroutine updates instrument state while the scan goroutine reads it;
// run with -race.
func TestDetector_ConcurrentScanAndUpdates(t *testing.T) {
	detector, registry, clock, _ := setupTestDetector(t)
	key, state := warmInstrument(registry, "AAPL", t0)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		tick := t0
		for i := 0; i < 500; i++ {
			tick = tick.Add(time.Second)
			detector.ObserveTick(context.Background(), key, state, tick)
			state.Lock()
			state.Gaps.Observe(1000)
			state.LastTickTime = tick
			state.Unlock()
			registry.GetOrCreate(model.InstrumentKey{Source: testExchange, InstrumentIdentifier: "NEW" + string(rune('A'+i%26))})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			clock.Advance(testExchange, t0.Add(time.Duration(i)*time.Second))
			detector.scan(context.Background(), model.TriggerScan)
		}
	}()

	wg.Wait()
}

func TestDetermineLatencyLevel(t *testing.T) {
	tests := []struct {
		name          string
		elapsedMs     float64
		thresholdMs   float64
		expectedLevel string
	}{
		{"just past the threshold", 5000, 4500, "LOW"},
		{"3x is still low", 13500, 4500, "LOW"},
		{"over 3x", 14000, 4500, "MEDIUM"},
		{"10x is still medium", 45000, 4500, "MEDIUM"},
		{"over 10x", 46000, 4500, "SEVERE"},
		{"no threshold", 1000, 0, "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if level := model.DetermineLatencyLevel(tt.elapsedMs, tt.thresholdMs); level != tt.expectedLevel {
				t.Errorf("Expected %s, got %s", tt.expectedLevel, level)
			}
		})
	}
}

// One partition can be minutes ahead of another. The scan must judge an instrument
// against the slowest partition, not the fastest, or lag looks like silence.
func TestDetector_LagBetweenPartitionsIsNotSilence(t *testing.T) {
	detector, registry, clock, emitter := setupTestDetector(t)
	_, state := warmInstrument(registry, "SLOWPART", t0) // its partition has delivered up to t0

	// another partition of the same exchange is ten minutes ahead
	fast := model.WithPartition(context.Background(), 1)
	slow := model.WithPartition(context.Background(), 0)
	keyFast := model.InstrumentKey{Source: testExchange, InstrumentIdentifier: "FASTPART"}
	detector.ObserveTick(fast, keyFast, registry.GetOrCreate(keyFast), t0.Add(10*time.Minute))
	detector.ObserveTick(slow, model.InstrumentKey{Source: testExchange, InstrumentIdentifier: "SLOWPART"}, state, t0.Add(time.Second))
	state.Lock()
	state.LastTickTime = t0.Add(time.Second)
	state.Unlock()

	detector.scan(context.Background(), model.TriggerScan)
	if emitter.count() != 0 {
		t.Fatalf("an instrument waiting in a slower partition is not silent, got %d alerts", emitter.count())
	}

	// once the slow partition itself has moved on past the threshold without the instrument
	// ticking, it is silent
	other := model.InstrumentKey{Source: testExchange, InstrumentIdentifier: "OTHERSLOW"}
	detector.ObserveTick(slow, other, registry.GetOrCreate(other), t0.Add(2*time.Minute))
	if now, _ := clock.Now(testExchange); !now.Equal(t0.Add(2 * time.Minute)) {
		t.Fatalf("setup: the exchange clock should now be the slow partition's 2 min, got %v", now)
	}
	detector.scan(context.Background(), model.TriggerScan)
	if emitter.count() != 1 || emitter.alerts[0].Instrument != "SLOWPART" {
		t.Errorf("SLOWPART should now alert, got %d alerts", emitter.count())
	}
}
