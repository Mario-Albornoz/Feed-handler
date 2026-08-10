// Package silence implements the Phase 3 detection mechanism: feed silence monitoring
// using learned per-instrument, per-session baselines with no static thresholds.
package silence

import (
	"context"
	"log"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

type AlertEmitter interface {
	WriteAlert(ctx context.Context, alert *model.SilenceAlert) error
}

type Detector struct {
	registry      *model.InstrumentRegistry
	resolver      *model.SessionResolver
	alertEmitter  AlertEmitter
	gapMultiplier float64
	checkInterval time.Duration
}

func NewDetector(
	registry *model.InstrumentRegistry,
	resolver *model.SessionResolver,
	alertEmitter AlertEmitter,
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
	alertCount := 0

	instruments := d.registry.All()

	for key, state := range instruments {
		currentBucket, err := d.resolver.ResolveSessionBucket(now, key.Source)
		if err != nil {
			log.Printf("Failed to resolve session for %s/%s: %v", key.Source, key.InstrumentIdentifier, err)
			continue
		}

		relevantStats, _ := state.GetStateForBucket(currentBucket)

		if relevantStats.ObservationCount < relevantStats.MinObservations {
			continue
		}

		if relevantStats.SlowMeanIntertick <= 0 {
			continue
		}

		if state.LastTickTime.IsZero() {
			continue
		}

		elapsed := now.Sub(state.LastTickTime)
		elapsedMs := float64(elapsed.Milliseconds())

		// Calculate threshold: gap_multiplier × learned mean interval
		thresholdMs := d.gapMultiplier * relevantStats.SlowMeanIntertick

		// Alert if silence exceeds threshold
		if elapsedMs > thresholdMs {
			alert := &model.SilenceAlert{
				Exchange:         key.Source,
				Instrument:       key.InstrumentIdentifier,
				AlertType:        "SILENCE",
				LastSeen:         state.LastTickTime,
				ElapsedMs:        int64(elapsedMs),
				ExpectedInterval: relevantStats.SlowMeanIntertick,
				LatencyLevel:     model.DetermineLatencyLevel(elapsedMs, relevantStats.SlowMeanIntertick),
				Timestamp:        now,
			}

			if err := d.emitAlert(ctx, alert); err != nil {
				log.Printf("Failed to emit silence alert for %s/%s: %v",
					key.Source, key.InstrumentIdentifier, err)
			} else {
				alertCount++
			}
		}
	}

	if alertCount > 0 {
		log.Printf("Silence scan complete: %d alerts emitted (scanned %d instruments)",
			alertCount, len(instruments))
	}
}

// emitAlert writes the silence alert to Kafka
func (d *Detector) emitAlert(ctx context.Context, alert *model.SilenceAlert) error {
	return d.alertEmitter.WriteAlert(ctx, alert)
}
