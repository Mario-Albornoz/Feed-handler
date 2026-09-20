package model

import (
	"sync"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
)

// InstrumentState is written by the consumer goroutine and read by the silence
// detector's scan goroutine. Writers and readers of the tick times, the rolling
// statistics and SilenceAlertedFor must hold Lock. The mutex is unexported, so the
// gob registry snapshot ignores it.
type InstrumentState struct {
	mu sync.Mutex

	// LastTickTime is the previous message on the feature clock (the whole-second update
	// time, see RawTick.FeatureTime); it drives the timing statistics and silence.
	LastTickTime     time.Time
	PreviousTickTime time.Time
	// LastTradingTime is the previous message's TradingTime, the reference for the
	// validator's timestamp-inversion rule.
	LastTradingTime time.Time
	// PrevLastTradedPrice is the price of the instrument's last *trade*. Messages
	// without a trade (empty last price) leave it untouched, i.e. the last traded price
	// is carried forward.
	PrevLastTradedPrice float64
	StatsBySession      map[SessionBucket]*stats.RollingStats
	AllSessionStats     *stats.RollingStats

	// Gaps is the distribution of the instrument's gaps between messages, which the
	// silence detector compares silences against.
	Gaps stats.GapQuantile

	// SilenceAlertedFor is the LastTickTime of the silence already reported for
	// this instrument, so each silence raises a single alert.
	SilenceAlertedFor time.Time
}

func (instrumentState *InstrumentState) Lock()   { instrumentState.mu.Lock() }
func (instrumentState *InstrumentState) Unlock() { instrumentState.mu.Unlock() }

func NewInstrumentState(fastWindowTicks float64, slowWindowticks float64, cusumSlack float64) *InstrumentState {

	statsBySession := make(map[SessionBucket]*stats.RollingStats)

	for bucket := PreMarket; bucket <= Weekend; bucket++ {
		statsBySession[bucket] = stats.NewRollingStats(fastWindowTicks, slowWindowticks, cusumSlack)
	}

	return &InstrumentState{
		LastTickTime:        time.Time{},
		PreviousTickTime:    time.Time{},
		PrevLastTradedPrice: 0.0,
		StatsBySession:      statsBySession,
		AllSessionStats:     stats.NewRollingStats(fastWindowTicks, slowWindowticks, cusumSlack),
		Gaps:                stats.NewGapQuantile(slowWindowticks),
	}
}

// SetMinPriceObservations sets the price warm-up requirement on every statistics set.
func (instrumentState *InstrumentState) SetMinPriceObservations(n int64) {
	instrumentState.AllSessionStats.MinPriceObservations = n
	for _, s := range instrumentState.StatsBySession {
		s.MinPriceObservations = n
	}
}

// SameDay reports whether two times fall on the same calendar day. Times in the data
// are exchange-local wall-clock time labelled UTC, so the day is taken in UTC.
func SameDay(a, b time.Time) bool {
	ay, am, ad := a.UTC().Date()
	by, bm, bd := b.UTC().Date()
	return ay == by && am == bm && ad == bd
}

func (instrumentState *InstrumentState) GetStateForBucket(bucket SessionBucket) (*stats.RollingStats, bool) {
	sessionStats := instrumentState.StatsBySession[bucket]

	if sessionStats.ObservationCount >= sessionStats.MinObservations {
		return sessionStats, false
	}
	return instrumentState.AllSessionStats, true
}

// GetPriceStateForBucket is GetStateForBucket for the price-step statistics. Trades are
// rare compared with messages, so a session bucket can have warm timing statistics and
// cold price statistics; the two are selected independently.
func (instrumentState *InstrumentState) GetPriceStateForBucket(bucket SessionBucket) (*stats.RollingStats, bool) {
	sessionStats := instrumentState.StatsBySession[bucket]

	if sessionStats.PriceObservationCount >= sessionStats.MinPriceObservations {
		return sessionStats, false
	}
	return instrumentState.AllSessionStats, true
}

type InstrumentKey struct {
	Source               string
	InstrumentIdentifier string
}

func NewInstrumentKey(exchange string, instrument string) *InstrumentKey {
	return &InstrumentKey{
		Source:               exchange,
		InstrumentIdentifier: instrument,
	}
}
