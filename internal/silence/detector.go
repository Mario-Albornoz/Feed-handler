// Package silence implements the Phase 3 detection mechanism: feed silence monitoring
// against each instrument's own learned gap distribution, with no static thresholds.
//
// Silence is a timing problem, so it is detected in event time and independently of
// the RRCF feature pipeline. An instrument is silent when the feed's event clock has
// moved past its last message by more than a high quantile (default the 99.9th) of the
// instrument's own past gaps. Two paths see this:
//
//   - the periodic scan (a goroutine on a wall-clock ticker), for instruments that
//     are quiet right now;
//   - ObserveTick, for an instrument that ticks again after a gap the scan missed.
//     Under an accelerated replay a silence can start and end between two scans, so
//     this path is what makes detection independent of replay speed.
//
// Both paths apply the same rule and record DetectedAt = LastSeen + threshold, the
// first event time at which the silence exceeded the threshold, so an alert's
// timestamp does not depend on which path found it or on the replay speed. A gap that
// spans a change of calendar day is the overnight closure and is never a silence.
//
// The clock is the whole-second update time (model.RawTick.FeatureTime), which is
// monotone; the millisecond TradingTime is not.
package silence

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

type AlertEmitter interface {
	WriteAlert(ctx context.Context, alert *model.SilenceAlert) error
}

// Config holds the silence rule's parameters.
type Config struct {
	// Quantile of the instrument's own gaps used as the silence threshold (e.g. 0.999).
	Quantile float64
	// Multiplier scales that quantile (1 = the quantile itself). Alerts are logged at the
	// configured value; a stricter multiplier can be applied afterwards from the log
	// (elapsed / threshold).
	Multiplier float64
	// MinObservations is how many gaps an instrument needs before it can alert.
	MinObservations int64
	// MinThresholdMs is a floor for the threshold: the resolution of the clock (whole
	// seconds), below which a gap cannot be told from rounding. It is not a tuned value.
	MinThresholdMs float64
}

type Detector struct {
	registry      *model.InstrumentRegistry
	clock         *model.EventClock
	alertEmitter  AlertEmitter
	cfg           Config
	checkInterval time.Duration
}

func NewDetector(
	registry *model.InstrumentRegistry,
	clock *model.EventClock,
	alertEmitter AlertEmitter,
	cfg Config,
	checkInterval time.Duration,
) *Detector {
	return &Detector{
		registry:      registry,
		clock:         clock,
		alertEmitter:  alertEmitter,
		cfg:           cfg,
		checkInterval: checkInterval,
	}
}

// Run starts the silence detection loop. Blocks until ctx is cancelled, then runs a
// final scan against the last event-time watermark so silences still open at the end
// of the stream are reported. Should be run in a separate goroutine.
func (d *Detector) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.checkInterval)
	defer ticker.Stop()

	log.Printf("Silence detector started (check interval: %v, threshold: %.4g quantile x %.2f, min %d observations)",
		d.checkInterval, d.cfg.Quantile, d.cfg.Multiplier, d.cfg.MinObservations)

	for {
		select {
		case <-ctx.Done():
			log.Println("Silence detector stopping...")
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			d.scan(flushCtx, model.TriggerFlush)
			cancel()
			return ctx.Err()
		case <-ticker.C:
			d.scan(ctx, model.TriggerScan)
		}
	}
}

// ObserveTick advances the event clock and checks whether the message ends a silence
// that has not been reported yet. It must be called for every accepted message, with its
// feature time, before the instrument's statistics and LastTickTime are updated.
func (d *Detector) ObserveTick(ctx context.Context, key model.InstrumentKey, state *model.InstrumentState, tickTime time.Time) {
	partition, _ := model.PartitionFrom(ctx)
	d.clock.AdvancePartition(key.Source, partition, tickTime)
	d.check(ctx, key, state, tickTime, model.TriggerResume)
}

// scan checks every instrument against its exchange's current event-time watermark.
func (d *Detector) scan(ctx context.Context, trigger string) {
	alertCount := 0

	instruments := d.registry.All()
	for key, state := range instruments {
		until, ok := d.clock.Now(key.Source)
		if !ok {
			continue
		}
		if d.check(ctx, key, state, until, trigger) {
			alertCount++
		}
	}

	if alertCount > 0 {
		log.Printf("Silence scan (%s) complete: %d alerts emitted (scanned %d instruments)",
			trigger, alertCount, len(instruments))
	}
}

// check emits an alert if the instrument has been silent until `until` for longer
// than its threshold and that silence has not been reported yet. It reports whether
// an alert was raised.
func (d *Detector) check(ctx context.Context, key model.InstrumentKey, state *model.InstrumentState, until time.Time, trigger string) bool {
	state.Lock()
	alert := d.evaluate(key, state, until, trigger)
	state.Unlock()

	if alert == nil {
		return false
	}

	if err := d.alertEmitter.WriteAlert(ctx, alert); err != nil {
		log.Printf("Failed to emit silence alert for %s/%s: %v", key.Source, key.InstrumentIdentifier, err)
		return false
	}
	return true
}

// evaluate applies the silence rule. The caller holds the state lock. A silence is
// identified by the message that preceded it, so a second alert for the same silence
// is suppressed.
func (d *Detector) evaluate(key model.InstrumentKey, state *model.InstrumentState, until time.Time, trigger string) *model.SilenceAlert {
	last := state.LastTickTime
	if last.IsZero() || !until.After(last) {
		return nil
	}
	// The gap across a change of day is the overnight closure, not a silence.
	if !model.SameDay(last, until) {
		return nil
	}
	if state.SilenceAlertedFor.Equal(last) {
		return nil
	}
	if state.Gaps.N < d.cfg.MinObservations {
		return nil
	}

	thresholdMs := math.Max(d.cfg.Multiplier*state.Gaps.Quantile(d.cfg.Quantile), d.cfg.MinThresholdMs)
	elapsedMs := float64(until.Sub(last)) / float64(time.Millisecond)
	if elapsedMs <= thresholdMs {
		return nil
	}

	state.SilenceAlertedFor = last

	return &model.SilenceAlert{
		Exchange:         key.Source,
		Instrument:       key.InstrumentIdentifier,
		AlertType:        "SILENCE",
		LastSeen:         last,
		ElapsedMs:        int64(elapsedMs),
		ExpectedInterval: state.AllSessionStats.SlowMeanIntertick,
		LatencyLevel:     model.DetermineLatencyLevel(elapsedMs, thresholdMs),
		Timestamp:        time.Now(),
		DetectedAt:       last.Add(time.Duration(thresholdMs * float64(time.Millisecond))),
		ThresholdMs:      thresholdMs,
		ObservedAt:       until,
		Trigger:          trigger,
	}
}
