// Package processing contains logic for processing ticks from the kafka consumer
package processing

import (
	"context"
	"fmt"
	"log"
	"math"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

// VectorEmitter is an interface for emitting normalized vectors
// This breaks the import cycle with the kafka package
type VectorEmitter interface {
	WriteVector(ctx context.Context, vector *model.NormalizedVector) error
}

type FeedProcessor struct {
	config             *config.AggregatorConfig
	sessionResolver    *model.SessionResolver
	instrumentRegistry *model.InstrumentRegistry
	producer           VectorEmitter
}

func NewFeedProcessor(aggregatorConfig config.AggregatorConfig, resolver *model.SessionResolver, instrumentRegistry *model.InstrumentRegistry, producer VectorEmitter) *FeedProcessor {
	return &FeedProcessor{
		config:             &aggregatorConfig,
		sessionResolver:    resolver,
		instrumentRegistry: instrumentRegistry,
		producer:           producer,
	}
}

func (fp *FeedProcessor) ProcessRawTicks(ctx context.Context, rawTick *model.RawTick) error {

	// we will skip all instruments that are not equity, since we currently only support equity instruments
	if rawTick.SecType != "E" {
		return nil
	}
	// STEP 1: Create instrument key and get or create its state
	key := model.InstrumentKey{
		Source:               rawTick.Exchange,
		InstrumentIdentifier: rawTick.ID,
	}
	instrumentState := fp.instrumentRegistry.GetOrCreate(key)

	// STEP 2: Resolve the session bucket for this tick's timestamp
	bucket, err := fp.sessionResolver.ResolveSessionBucket(rawTick.TradingTime, rawTick.Exchange)
	if err != nil {
		log.Printf("Error retriving bucket for instrument id %v", rawTick.ID)
	}

	// STEP 3: Derive the four raw values needed for rolling statistics:
	var interTick float64

	if instrumentState.LastTickTime.IsZero() {
		interTick = 0.0
	} else {
		interTick = float64(rawTick.TradingTime.Sub(instrumentState.LastTickTime).Milliseconds())
	}
	// spread: The bid-ask spread (ask - bid)
	spread := rawTick.Ask - rawTick.Bid

	// priceStep: Absolute change in mid-price since the previous tick
	curentMid := (rawTick.Bid + rawTick.Ask) / 2.0
	priceStep := math.Abs(curentMid - instrumentState.PrevMid)

	// volume: Trading volume from the tick
	volume := rawTick.TotalVolume

	// STEP 4: Update rolling statistics
	// Update session-specific stats for the resolved bucket:
	sessionStats := instrumentState.StatsBySession[bucket]
	sessionStats.Update(interTick, spread, priceStep, volume)

	// Update all-sessions combined stats (for fallback):
	instrumentState.AllSessionStats.Update(interTick, spread, priceStep, volume)

	// STEP 5: Get the appropriate stats for z-score calculation (with fallback)
	relevantStats, usedFallBack := instrumentState.GetStateForBucket(bucket)
	fallbackFlag := 0
	if usedFallBack {
		fallbackFlag = 1
	}

	// STEP 6: Calculate z-scores using the relevant stats
	zFastIntertick, zFastSpread, zFastPriceStep, zFastVolume,
		zSlowIntertick, zSlowSpread, zSlowPriceStep, zSlowVolume :=
		relevantStats.ZScores(interTick, spread, priceStep, volume)

	// STEP 7: Get CUSUM values (already computed by Update, just read them)
	cusumIntertick := relevantStats.CusumIntertick
	cusumSpread := relevantStats.CusumSpread
	cusumPriceStep := relevantStats.CusumPriceStep
	cusumVolume := relevantStats.CusumVolume

	// STEP 8: Calculate additional flags
	gapFlag := relevantStats.GapFlag(interTick)

	quoteInv := 0
	if rawTick.Bid >= rawTick.Ask {
		quoteInv = 1
	}

	warmupFlag := 0
	if relevantStats.ObservationCount < relevantStats.MinObservations {
		warmupFlag = 1
	}

	// STEP 9: Create and emit NormalizedVector
	vector := &model.NormalizedVector{
		Timestamp:  rawTick.TradingTime,
		Exchange:   rawTick.Exchange,
		Instrument: rawTick.ID,
		Class:      rawTick.SecType,
		ModelKey:   rawTick.SecType,

		ZIntertickFast: zFastIntertick,
		ZSpreadFast:    zFastSpread,
		ZPriceStepFast: zFastPriceStep,
		ZVolumeFast:    zFastVolume,

		ZIntertickSlow: zSlowIntertick,
		ZSpreadSlow:    zSlowSpread,
		ZPriceStepSlow: zSlowPriceStep,
		ZVolumeSlow:    zSlowVolume,

		CusumIntertick: cusumIntertick,
		CusumSpread:    cusumSpread,
		CusumPriceStep: cusumPriceStep,
		CusumVolume:    cusumVolume,

		GapFlag:             gapFlag,
		QuoteInv:            quoteInv,
		WarmupFlag:          warmupFlag,
		SessionFallbackFlag: fallbackFlag,
	}

	// STEP 10: Send to Kafka producer
	if fp.producer != nil {
		err = fp.producer.WriteVector(ctx, vector)
		if err != nil {
			return fmt.Errorf("failed to write vector: %w", err)
		}
	}

	// STEP 11: Update instrument state for next tick
	instrumentState.LastTickTime = rawTick.TradingTime
	instrumentState.PrevMid = curentMid

	return nil
}
