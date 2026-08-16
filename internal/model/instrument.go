package model

import (
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
)

type InstrumentState struct {
	LastTickTime        time.Time
	LastTickReceivedAt  time.Time
	PrevLastTradedPrice float64
	StatsBySession      map[SessionBucket]*stats.RollingStats
	AllSessionStats     *stats.RollingStats
}

func NewInstrumentState(fastWindowTicks float64, slowWindowticks float64, cusumSlack float64) *InstrumentState {

	statsBySession := make(map[SessionBucket]*stats.RollingStats)

	for bucket := PreMarket; bucket <= Weekend; bucket++ {
		statsBySession[bucket] = stats.NewRollingStats(fastWindowTicks, slowWindowticks, cusumSlack)
	}

	return &InstrumentState{
		LastTickTime:        time.Time{},
		PrevLastTradedPrice: 0.0,
		StatsBySession:      statsBySession,
		AllSessionStats:     stats.NewRollingStats(fastWindowTicks, slowWindowticks, cusumSlack),
	}
}

func (instrumentState *InstrumentState) GetStateForBucket(bucket SessionBucket) (*stats.RollingStats, bool) {
	sessionStats := instrumentState.StatsBySession[bucket]

	if sessionStats.ObservationCount >= sessionStats.MinObservations {
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
