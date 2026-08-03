// Package stats provides rolling statistical analysis for market data streams.
// It maintains exponential moving averages, variances, z-scores, and CUSUM
// change detection across multiple timescales for anomaly detection.
package stats

import (
	"math"
	"time"
)

// RollingStats maintains two-timescale exponential moving statistics
// and CUSUM values for a single (exchange, instrument) pair.
// All fields are computed independently for fast and slow baselines.
type RollingStats struct {
	ObservationCount int64

	FastMeanIntertick float64
	FastVarIntertick  float64
	FastMeanSpread    float64
	FastVarSpread     float64
	FastMeanPriceStep float64
	FastVarPriceStep  float64
	FastMeanVolume    float64
	FastVarVolume     float64

	SlowMeanIntertick float64
	SlowVarIntertick  float64
	SlowMeanSpread    float64
	SlowVarSpread     float64
	SlowMeanPriceStep float64
	SlowVarPriceStep  float64
	SlowMeanVolume    float64
	SlowVarVolume     float64

	CusumIntertick float64
	CusumSpread    float64
	CusumPriceStep float64
	CusumVolume    float64

	LastTickTime time.Time

	PrevMid float64

	//config fields
	FastAlpha       float64
	SlowAlpha       float64
	CusumSlack      float64
	GapMultiplier   float64
	MinObservations int64
}

// NewRollingStats creates a new instance with alpha values derived from
// the desired effective window sizes in ticks.
// fastWindowTicks: target window for fast baseline in ticks
// slowWindowTicks: target window for slow baseline in ticks
// cusumSlack: drift allowance for CUSUM algorithm
func NewRollingStats(fastWindowTicks, slowWindowTicks, cusumSlack float64) *RollingStats {
	// EMA alpha from window: alpha = 2 / (N + 1) where N = window in ticks
	return &RollingStats{
		FastAlpha:       2.0 / (float64(fastWindowTicks) + 1),
		SlowAlpha:       2.0 / (float64(slowWindowTicks) + 1),
		CusumSlack:      cusumSlack,
		GapMultiplier:   5.0,
		MinObservations: 50,
	}
}

// Update processes a single new observation and updates all statistics.
// intertick: milliseconds since last tick for this instrument
// spread: ask - bid
// priceStep: abs(mid - prevMid)
// volume: reported volume on this tick
func (r *RollingStats) Update(intertick, spread, priceStep, volume float64) {
	r.ObservationCount++

	r.FastMeanIntertick, r.FastVarIntertick = updateEMA(r.FastMeanIntertick, r.FastVarIntertick, intertick, r.FastAlpha)
	r.FastMeanSpread, r.FastVarSpread = updateEMA(r.FastMeanSpread, r.FastVarSpread, spread, r.FastAlpha)
	r.FastMeanPriceStep, r.FastVarPriceStep = updateEMA(r.FastMeanPriceStep, r.FastVarPriceStep, priceStep, r.FastAlpha)
	r.FastMeanVolume, r.FastVarVolume = updateEMA(r.FastMeanVolume, r.FastVarVolume, volume, r.FastAlpha)

	r.SlowMeanIntertick, r.SlowVarIntertick = updateEMA(r.SlowMeanIntertick, r.SlowVarIntertick, intertick, r.SlowAlpha)
	r.SlowMeanSpread, r.SlowVarSpread = updateEMA(r.SlowMeanSpread, r.SlowVarSpread, spread, r.SlowAlpha)
	r.SlowMeanPriceStep, r.SlowVarPriceStep = updateEMA(r.SlowMeanPriceStep, r.SlowVarPriceStep, priceStep, r.SlowAlpha)
	r.SlowMeanVolume, r.SlowVarVolume = updateEMA(r.SlowMeanVolume, r.SlowVarVolume, volume, r.SlowAlpha)

	zSlowIntertick := zScore(intertick, r.SlowMeanIntertick, r.SlowVarIntertick)
	zSlowSpread := zScore(spread, r.SlowMeanSpread, r.SlowVarSpread)
	zSlowPriceStep := zScore(priceStep, r.SlowMeanPriceStep, r.SlowVarPriceStep)
	zSlowVolume := zScore(volume, r.SlowMeanVolume, r.SlowVarVolume)

	r.CusumIntertick = math.Max(0, r.CusumIntertick+zSlowIntertick-r.CusumSlack)
	r.CusumSpread = math.Max(0, r.CusumSpread+zSlowSpread-r.CusumSlack)
	r.CusumPriceStep = math.Max(0, r.CusumPriceStep+zSlowPriceStep-r.CusumSlack)
	r.CusumVolume = math.Max(0, r.CusumVolume+zSlowVolume-r.CusumSlack)
}

// ZScores should be called after Update().
func (r *RollingStats) ZScores(intertick, spread, priceStep, volume float64) (
	zFastIntertick, zFastSpread, zFastPriceStep, zFastVolume,
	zSlowIntertick, zSlowSpread, zSlowPriceStep, zSlowVolume float64,
) {
	zFastIntertick = zScore(intertick, r.FastMeanIntertick, r.FastVarIntertick)
	zFastSpread = zScore(spread, r.FastMeanSpread, r.FastVarSpread)
	zFastPriceStep = zScore(priceStep, r.FastMeanPriceStep, r.FastVarPriceStep)
	zFastVolume = zScore(volume, r.FastMeanVolume, r.FastVarVolume)

	zSlowIntertick = zScore(intertick, r.SlowMeanIntertick, r.SlowVarIntertick)
	zSlowSpread = zScore(spread, r.SlowMeanSpread, r.SlowVarSpread)
	zSlowPriceStep = zScore(priceStep, r.SlowMeanPriceStep, r.SlowVarPriceStep)
	zSlowVolume = zScore(volume, r.SlowMeanVolume, r.SlowVarVolume)
	return
}

// IsWarm returns true when enough observations have been collected
// for rolling statistics to be reliable.
func (r *RollingStats) IsWarm() bool {
	return r.ObservationCount >= r.MinObservations
}

// GapFlag returns 1 if the current intertick interval exceeds
// GapMultiplier times the fast rolling mean. 0 otherwise.
func (r *RollingStats) GapFlag(intertickMs float64) int {
	if r.FastMeanIntertick > 0 && intertickMs > r.GapMultiplier*r.FastMeanIntertick {
		return 1
	}
	return 0
}

// Helpers

func updateEMA(mean, variance, newVal, alpha float64) (newMean, newVar float64) {
	diff := newVal - mean
	newMean = mean + alpha*diff
	newVar = (1 - alpha) * (variance + alpha*diff*diff)
	return
}

func zScore(value, mean, variance float64) float64 {
	std := math.Sqrt(variance)
	if std < 1e-10 {
		return 0
	}
	return (value - mean) / std
}
