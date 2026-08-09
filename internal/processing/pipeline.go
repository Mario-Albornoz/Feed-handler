package processing

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
)

var (
	// ErrSkipTick signals that processing should stop but is not an error
	ErrSkipTick = errors.New("skip tick")
)

// ProcessingState holds all state as a tick flows through the pipeline
type ProcessingState struct {
	Tick *model.RawTick

	SessionBucket model.SessionBucket

	InstrumentKey   model.InstrumentKey
	InstrumentState *model.InstrumentState

	Intertick  float64
	Spread     float64
	PriceStep  float64
	Volume     float64
	CurrentMid float64

	RelevantStats *stats.RollingStats
	UsedFallback  bool

	ZFastIntertick float64
	ZFastSpread    float64
	ZFastPriceStep float64
	ZFastVolume    float64
	ZSlowIntertick float64
	ZSlowSpread    float64
	ZSlowPriceStep float64
	ZSlowVolume    float64

	CusumIntertick float64
	CusumSpread    float64
	CusumPriceStep float64
	CusumVolume    float64

	GapFlag             int
	QuoteInv            int
	WarmupFlag          int
	SessionFallbackFlag int

	Vector *model.NormalizedVector
}

// TickProcessor defines a single step in the processing pipeline it is implemented by all the Processors
type TickProcessor interface {
	Process(ctx context.Context, state *ProcessingState) error
}

type EquityFilterProcessor struct{}

func (p *EquityFilterProcessor) Process(ctx context.Context, state *ProcessingState) error {
	if state.Tick.SecType != "E" {
		return ErrSkipTick
	}
	return nil
}

type SessionResolverProcessor struct {
	resolver *model.SessionResolver
}

func (p *SessionResolverProcessor) Process(ctx context.Context, state *ProcessingState) error {
	bucket, err := p.resolver.ResolveSessionBucket(state.Tick.TradingTime, state.Tick.Exchange)
	if err != nil {
		return fmt.Errorf("failed to resolve session: %w", err)
	}
	state.SessionBucket = bucket
	return nil
}

type InstrumentLookupProcessor struct {
	registry *model.InstrumentRegistry
}

func (p *InstrumentLookupProcessor) Process(ctx context.Context, state *ProcessingState) error {
	state.InstrumentKey = model.InstrumentKey{
		Source:               state.Tick.Exchange,
		InstrumentIdentifier: state.Tick.ID,
	}
	state.InstrumentState = p.registry.GetOrCreate(state.InstrumentKey)
	return nil
}

type MetricsCalculatorProcessor struct{}

func (p *MetricsCalculatorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	if state.InstrumentState.LastTickTime.IsZero() {
		state.Intertick = 0.0
	} else {
		state.Intertick = float64(state.Tick.TradingTime.Sub(state.InstrumentState.LastTickTime).Milliseconds())
	}

	state.Spread = state.Tick.Ask - state.Tick.Bid

	state.CurrentMid = (state.Tick.Bid + state.Tick.Ask) / 2.0
	state.PriceStep = math.Abs(state.CurrentMid - state.InstrumentState.PrevMid)

	state.Volume = state.Tick.TotalVolume

	return nil
}

// StatsUpdaterProcessor updates both session-specific and all-session statistics
type StatsUpdaterProcessor struct{}

func (p *StatsUpdaterProcessor) Process(ctx context.Context, state *ProcessingState) error {
	sessionStats := state.InstrumentState.StatsBySession[state.SessionBucket]
	sessionStats.Update(state.Intertick, state.Spread, state.PriceStep, state.Volume)

	state.InstrumentState.AllSessionStats.Update(state.Intertick, state.Spread, state.PriceStep, state.Volume)

	return nil
}

// FallbackSelectorProcessor determines which stats to use (session-specific or fallback)
type FallbackSelectorProcessor struct{}

func (p *FallbackSelectorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	relevantStats, usedFallback := state.InstrumentState.GetStateForBucket(state.SessionBucket)
	state.RelevantStats = relevantStats
	state.UsedFallback = usedFallback

	if usedFallback {
		state.SessionFallbackFlag = 1
	} else {
		state.SessionFallbackFlag = 0
	}

	return nil
}

// ZScoreCalculatorProcessor computes z-scores using the selected stats
type ZScoreCalculatorProcessor struct{}

func (p *ZScoreCalculatorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	state.ZFastIntertick, state.ZFastSpread, state.ZFastPriceStep, state.ZFastVolume,
		state.ZSlowIntertick, state.ZSlowSpread, state.ZSlowPriceStep, state.ZSlowVolume =
		state.RelevantStats.ZScores(state.Intertick, state.Spread, state.PriceStep, state.Volume)

	return nil
}

// CusumExtractorProcessor extracts CUSUM values from the stats
type CusumExtractorProcessor struct{}

func (p *CusumExtractorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	state.CusumIntertick = state.RelevantStats.CusumIntertick
	state.CusumSpread = state.RelevantStats.CusumSpread
	state.CusumPriceStep = state.RelevantStats.CusumPriceStep
	state.CusumVolume = state.RelevantStats.CusumVolume
	return nil
}

// FlagCalculatorProcessor computes all flags
type FlagCalculatorProcessor struct{}

func (p *FlagCalculatorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	// GapFlag: 1 if intertick > 5x rolling mean
	state.GapFlag = state.RelevantStats.GapFlag(state.Intertick)

	// QuoteInv: 1 if bid >= ask (inverted quote)
	if state.Tick.Bid >= state.Tick.Ask {
		state.QuoteInv = 1
	} else {
		state.QuoteInv = 0
	}

	// WarmupFlag: 1 if not enough observations yet
	if state.RelevantStats.ObservationCount < state.RelevantStats.MinObservations {
		state.WarmupFlag = 1
	} else {
		state.WarmupFlag = 0
	}

	return nil
}

type VectorBuilderProcessor struct {
	config *config.AggregatorConfig
}

func (p *VectorBuilderProcessor) Process(ctx context.Context, state *ProcessingState) error {
	modelKey := state.Tick.SecType

	state.Vector = &model.NormalizedVector{
		Timestamp:  state.Tick.TradingTime,
		Exchange:   state.Tick.Exchange,
		Instrument: state.Tick.ID,
		Class:      state.Tick.SecType,
		ModelKey:   modelKey,

		ZIntertickFast: state.ZFastIntertick,
		ZSpreadFast:    state.ZFastSpread,
		ZPriceStepFast: state.ZFastPriceStep,
		ZVolumeFast:    state.ZFastVolume,

		ZIntertickSlow: state.ZSlowIntertick,
		ZSpreadSlow:    state.ZSlowSpread,
		ZPriceStepSlow: state.ZSlowPriceStep,
		ZVolumeSlow:    state.ZSlowVolume,

		CusumIntertick: state.CusumIntertick,
		CusumSpread:    state.CusumSpread,
		CusumPriceStep: state.CusumPriceStep,
		CusumVolume:    state.CusumVolume,

		GapFlag:             state.GapFlag,
		QuoteInv:            state.QuoteInv,
		WarmupFlag:          state.WarmupFlag,
		SessionFallbackFlag: state.SessionFallbackFlag,
	}

	return nil
}

// VectorEmitterProcessor writes the vector to Kafka
type VectorEmitterProcessor struct {
	producer VectorEmitter
}

func (p *VectorEmitterProcessor) Process(ctx context.Context, state *ProcessingState) error {
	if p.producer != nil {
		if err := p.producer.WriteVector(ctx, state.Vector); err != nil {
			return fmt.Errorf("failed to emit vector: %w", err)
		}
	}
	return nil
}

// StateUpdaterProcessor updates instrument state after successful processing
type StateUpdaterProcessor struct{}

func (p *StateUpdaterProcessor) Process(ctx context.Context, state *ProcessingState) error {
	state.InstrumentState.LastTickTime = state.Tick.TradingTime
	state.InstrumentState.PrevMid = state.CurrentMid
	return nil
}
