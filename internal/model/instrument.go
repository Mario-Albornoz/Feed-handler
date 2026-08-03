package model

import (
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
)

type InstrumentState struct {
	LastTickTime    time.Time
	PrevMid         float64
	StatsBySession  map[SessionBucket]*stats.RollingStats
	AllSessionStats *stats.RollingStats
}

func NewInstrumentState(fastWindowTicks float64, slowWindowticks float64, cusumSlack float64) *InstrumentState {

	statsBySession := make(map[SessionBucket]*stats.RollingStats)

	for bucket := PreMarket; bucket <= Weekend; bucket++ {
		statsBySession[bucket] = stats.NewRollingStats(fastWindowTicks, slowWindowticks, cusumSlack)
	}

	return &InstrumentState{
		LastTickTime:    time.Time{},
		PrevMid:         0.0,
		StatsBySession:  statsBySession,
		AllSessionStats: stats.NewRollingStats(float64(fastWindowTicks), float64(slowWindowticks), cusumSlack),
	}
}

type InstrumentKey struct {
	ExchangeName         string
	InstrumentIdentifier string
}

func NewInstrumentKey(exchangeName string, instrumentIdentifier string) *InstrumentKey {
	return &InstrumentKey{
		ExchangeName:         exchangeName,
		InstrumentIdentifier: instrumentIdentifier,
	}
}
