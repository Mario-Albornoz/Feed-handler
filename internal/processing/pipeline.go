package processing

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/validation"
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

	Intertick float64
	PriceStep float64

	// NewDay: this message is the first of a new calendar day for the instrument. The
	// gap since the previous day's last message is the overnight closure, not a
	// measurement of the feed, so it is not an observation.
	NewDay bool

	// HasTrade: the message carries a last traded price. HasPriceStep: it is a trade and
	// the instrument has a previous trade, so PriceStep (the change between the two
	// trades) is defined.
	HasTrade     bool
	HasPriceStep bool

	RelevantStats      *stats.RollingStats
	RelevantPriceStats *stats.RollingStats
	UsedFallback       bool

	ZFastIntertick float64
	ZFastPriceStep float64
	ZSlowIntertick float64
	ZSlowPriceStep float64

	CusumIntertick float64
	CusumPriceStep float64

	GapFlag             int
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
	bucket, err := p.resolver.ResolveSessionBucket(state.Tick.FeatureTime(), state.Tick.Exchange)
	if err != nil {
		return fmt.Errorf("failed to resolve session: %w", err)
	}
	state.SessionBucket = bucket
	return nil
}

// ValidatorProcessor rejects malformed ticks before they can touch instrument state
// or reach the detector. Rejected ticks are reported by the validator (to the
// evaluation log) and skipped.
//
// A rejected message is still a message: it proves the feed is alive. So it is shown to
// the silence detector and moves the instrument's last-seen time forward, but it adds no
// statistics observation and leaves the validator's reference time alone. (Without this a
// quarantined message looks like a gap in the feed and can raise a false silence alert.)
type ValidatorProcessor struct {
	validator *validation.Validator
	observer  SilenceObserver
}

func (p *ValidatorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	if p.validator.Validate(ctx, state.Tick, state.InstrumentState.LastTradingTime) {
		return nil
	}

	featureTime := state.Tick.FeatureTime()
	if p.observer != nil {
		p.observer.ObserveTick(ctx, state.InstrumentKey, state.InstrumentState, featureTime)
	}
	state.InstrumentState.Lock()
	if featureTime.After(state.InstrumentState.LastTickTime) {
		state.InstrumentState.LastTickTime = featureTime
	}
	state.InstrumentState.Unlock()
	return ErrSkipTick
}

// SilenceObserver is told about every accepted tick, before its statistics are
// updated, so silence can be measured against the event-time clock. It is
// implemented by the silence detector.
type SilenceObserver interface {
	ObserveTick(ctx context.Context, key model.InstrumentKey, state *model.InstrumentState, tickTime time.Time)
}

// SilenceObserverProcessor forwards accepted ticks to the silence detector. The
// silence rule itself lives in the detector, outside the feature pipeline.
type SilenceObserverProcessor struct {
	observer SilenceObserver
}

func (p *SilenceObserverProcessor) Process(ctx context.Context, state *ProcessingState) error {
	p.observer.ObserveTick(ctx, state.InstrumentKey, state.InstrumentState, state.Tick.FeatureTime())
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
	// Timing runs on the whole-second update time (FeatureTime), which is monotone; the
	// millisecond TradingTime jumps back and forth within a second.
	featureTime := state.Tick.FeatureTime()
	last := state.InstrumentState.LastTickTime
	state.NewDay = false
	switch {
	case last.IsZero():
		state.Intertick = 0.0
	case !model.SameDay(last, featureTime):
		state.Intertick = 0.0
		state.NewDay = true
	default:
		state.Intertick = float64(featureTime.Sub(last).Milliseconds())
	}

	// Most messages are quote updates whose last price is empty (0 after parsing). They
	// carry no price information, so the last traded price is carried forward and the
	// price step is defined only between two consecutive trades.
	state.HasTrade = state.Tick.LastTradedPrice > 0
	state.HasPriceStep = state.HasTrade && state.InstrumentState.PrevLastTradedPrice > 0
	state.PriceStep = 0
	if state.HasPriceStep {
		state.PriceStep = math.Abs(state.Tick.LastTradedPrice - state.InstrumentState.PrevLastTradedPrice)
	}

	return nil
}

// StatsUpdaterProcessor updates both session-specific and all-session statistics
type StatsUpdaterProcessor struct{}

func (p *StatsUpdaterProcessor) Process(ctx context.Context, state *ProcessingState) error {
	// The silence detector reads these statistics from its own goroutine.
	state.InstrumentState.Lock()
	defer state.InstrumentState.Unlock()

	// The first message of an instrument, and the first of a new day, have no usable
	// predecessor: their inter-tick interval (0, or the overnight closure) is not an
	// observation, and under a running average during warm-up it would dominate.
	if state.InstrumentState.LastTickTime.IsZero() || state.NewDay {
		return nil
	}

	sessionStats := state.InstrumentState.StatsBySession[state.SessionBucket]
	allStats := state.InstrumentState.AllSessionStats

	// Timing is observed on every message, price only between trades.
	sessionStats.UpdateIntertick(state.Intertick)
	allStats.UpdateIntertick(state.Intertick)
	state.InstrumentState.Gaps.Observe(state.Intertick)
	if state.HasPriceStep {
		sessionStats.UpdatePriceStep(state.PriceStep)
		allStats.UpdatePriceStep(state.PriceStep)
	}

	return nil
}

// FallbackSelectorProcessor determines which stats to use (session-specific or fallback)
type FallbackSelectorProcessor struct{}

func (p *FallbackSelectorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	relevantStats, usedFallback := state.InstrumentState.GetStateForBucket(state.SessionBucket)
	state.RelevantStats = relevantStats
	state.UsedFallback = usedFallback

	// Trades are rare, so a bucket can have warm timing statistics and cold price
	// statistics: the price statistics are selected on their own count.
	state.RelevantPriceStats, _ = state.InstrumentState.GetPriceStateForBucket(state.SessionBucket)

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
	state.ZFastIntertick, state.ZSlowIntertick = state.RelevantStats.IntertickZScores(state.Intertick)

	// A message without a price step says nothing about the price: neutral z-scores.
	state.ZFastPriceStep, state.ZSlowPriceStep = 0, 0
	if state.HasPriceStep {
		state.ZFastPriceStep, state.ZSlowPriceStep = state.RelevantPriceStats.PriceStepZScores(state.PriceStep)
	}

	return nil
}

// CusumExtractorProcessor extracts CUSUM values from the stats
type CusumExtractorProcessor struct{}

func (p *CusumExtractorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	state.CusumIntertick = state.RelevantStats.CusumIntertick
	state.CusumPriceStep = state.RelevantPriceStats.CusumPriceStep
	return nil
}

// FlagCalculatorProcessor computes all flags
type FlagCalculatorProcessor struct{}

func (p *FlagCalculatorProcessor) Process(ctx context.Context, state *ProcessingState) error {
	// GapFlag: 1 if intertick > 5x rolling mean
	state.GapFlag = state.RelevantStats.GapFlag(state.Intertick)

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
		Seq:        state.Tick.Seq,

		ZIntertickFast: state.ZFastIntertick,
		ZPriceStepFast: state.ZFastPriceStep,

		ZIntertickSlow: state.ZSlowIntertick,
		ZPriceStepSlow: state.ZSlowPriceStep,

		CusumIntertick: state.CusumIntertick,
		CusumPriceStep: state.CusumPriceStep,

		GapFlag:             state.GapFlag,
		HasTrade:            boolToInt(state.HasTrade),
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
	state.InstrumentState.Lock()
	defer state.InstrumentState.Unlock()

	state.InstrumentState.PreviousTickTime = state.InstrumentState.LastTickTime
	state.InstrumentState.LastTickTime = state.Tick.FeatureTime()
	state.InstrumentState.LastTradingTime = state.Tick.TradingTime
	if state.HasTrade {
		state.InstrumentState.PrevLastTradedPrice = state.Tick.LastTradedPrice
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
