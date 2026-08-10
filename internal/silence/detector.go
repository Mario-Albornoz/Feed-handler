// Package silence implements the Phase 3 detection mechanism: feed silence monitoring
// using learned per-instrument, per-session baselines with no static thresholds.
package silence

import (
	"context"
	"log"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/processing"
)

// Detector monitors instruments for unexpected silence by comparing elapsed time
// since last tick against each instrument's learned mean inter-tick interval.
// Uses proportional thresholds (gap_multiplier × learned_mean) so liquid and
// illiquid instruments are both monitored with the same universal parameters.
type Detector struct {
	registry      *model.InstrumentRegistry
	resolver      *model.SessionResolver
	alertEmitter  processing.VectorEmitter // Reuses the same interface, writes SilenceAlerts
	gapMultiplier float64
	checkInterval time.Duration
}

// NewDetector creates a new silence detector.
// gapMultiplier: universal ratio applied to learned mean (e.g., 5.0 = alert after 5x normal interval)
// checkInterval: how often to scan the registry (e.g., every 5 seconds)
func NewDetector(
	registry *model.InstrumentRegistry,
	resolver *model.SessionResolver,
	alertEmitter processing.VectorEmitter,
	gapMultiplier float64,
	checkInterval time.Duration,
) *Detector {
	return &Detector{
		registry:      registry,
		resolver:      resolver,
		alertEmitter:  alertEmitter,
		gapMultiplier: gapMultiplier,
		checkInterval: checkInterval,
	}
}

// Run starts the silence detection loop. Blocks until ctx is cancelled.
// Should be run in a separate goroutine.
func (d *Detector) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.checkInterval)
	defer ticker.Stop()

	log.Printf("Silence detector started (check interval: %v, gap multiplier: %.1f)",
		d.checkInterval, d.gapMultiplier)

	for {
		select {
		case <-ctx.Done():
			log.Println("Silence detector stopping...")
			return ctx.Err()
		case <-ticker.C:
			d.scan(ctx)
		}
	}
}

// scan performs one pass over all instruments in the registry,
// checking for silence that exceeds the learned threshold.
func (d *Detector) scan(ctx context.Context) {
	now := time.Now()

	for instrument, _ := range d.registry.All() {
		currentSession, err := d.resolver.ResolveSessionBucket(time.Now(), instrument.Source)
		if err != nil {
			log.Printf("Error retriving current session bucket for exchange %v : %v", instrument.Source, err)
		}
		instrumentState := d.registry.GetOrCreate(instrument)
		bucketSessionStat, isPresent := instrumentState.GetStateForBucket(currentSession)
		if !isPresent {
			log.Printf("No bucketSessionStats for %v", instrumentState)
			continue
		}
		if !bucketSessionStat.IsWarm() {
			continue
		} else if bucketSessionStat.ObservationCount < bucketSessionStat.MinObservations {
			continue
		} else if bucketSessionStat.SlowMeanIntertick <= 0 || bucketSessionStat.LastTickTime.IsZero() {
			continue
		}

		elapsed := now - bucketSessionStat.LastTickTime

		if elapsed > d.gapMultiplier {
			alert := model.NewAlert(instrument.InstrumentIdentifier, instrument.Source, model.Medium)
			d.alertEmitter.WriteAlert(ctx, alert)
		}
	}
	_ = now // Suppress unused warning until implementation
}

func (d Detector) determineLatencyLevel(elapsed time.Time) *model.LatencyLevels {
	if elapsed > d.gapMultiplier*3 {
	}
}
