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
	ObservationCount      int64
	PriceObservationCount int64

	FastMeanIntertick float64
	FastVarIntertick  float64
	FastMeanPriceStep float64
	FastVarPriceStep  float64

	SlowMeanIntertick float64
	SlowVarIntertick  float64
	SlowMeanPriceStep float64
	SlowVarPriceStep  float64

	CusumIntertick float64
	CusumPriceStep float64

	LastTickTime time.Time

	PrevLastTradedPrice float64

	FastAlpha            float64
	SlowAlpha            float64
	CusumSlack           float64
	GapMultiplier        float64
	MinObservations      int64
	MinPriceObservations int64
}

func NewRollingStats(fastWindowTicks, slowWindowTicks, cusumSlack float64) *RollingStats {
	// EMA alpha from window: alpha = 2 / (N + 1) where N = window in ticks
	return &RollingStats{
		FastAlpha:       2.0 / (float64(fastWindowTicks) + 1),
		SlowAlpha:       2.0 / (float64(slowWindowTicks) + 1),
		CusumSlack:      cusumSlack,
		GapMultiplier:   5.0,
		MinObservations: 50,

		MinPriceObservations: 20,
	}
}

func (r *RollingStats) UpdateIntertick(intertick float64) {
	r.ObservationCount++
	// Weight max(alpha, 1/n): a plain running average until 1/n < alpha, so the averages
	// are not biased towards their zero start early in an instrument's history.
	warmup := 1.0 / float64(r.ObservationCount)
	fastAlpha := math.Max(r.FastAlpha, warmup)
	slowAlpha := math.Max(r.SlowAlpha, warmup)

	zSlowIntertick := zScore(intertick, r.SlowMeanIntertick, r.SlowVarIntertick)
	r.CusumIntertick = math.Max(0, r.CusumIntertick+zSlowIntertick-r.CusumSlack)

	r.FastMeanIntertick, r.FastVarIntertick = updateEMA(r.FastMeanIntertick, r.FastVarIntertick, intertick, fastAlpha)
	r.SlowMeanIntertick, r.SlowVarIntertick = updateEMA(r.SlowMeanIntertick, r.SlowVarIntertick, intertick, slowAlpha)
}

func (r *RollingStats) UpdatePriceStep(priceStep float64) {
	r.PriceObservationCount++
	warmup := 1.0 / float64(r.PriceObservationCount)
	fastAlpha := math.Max(r.FastAlpha, warmup)
	slowAlpha := math.Max(r.SlowAlpha, warmup)

	zSlowPriceStep := zScore(priceStep, r.SlowMeanPriceStep, r.SlowVarPriceStep)
	r.CusumPriceStep = math.Max(0, r.CusumPriceStep+zSlowPriceStep-r.CusumSlack)

	r.FastMeanPriceStep, r.FastVarPriceStep = updateEMA(r.FastMeanPriceStep, r.FastVarPriceStep, priceStep, fastAlpha)
	r.SlowMeanPriceStep, r.SlowVarPriceStep = updateEMA(r.SlowMeanPriceStep, r.SlowVarPriceStep, priceStep, slowAlpha)
}

func (r *RollingStats) IntertickZScores(intertick float64) (zFast, zSlow float64) {
	return zScore(intertick, r.FastMeanIntertick, r.FastVarIntertick),
		zScore(intertick, r.SlowMeanIntertick, r.SlowVarIntertick)
}

func (r *RollingStats) PriceStepZScores(priceStep float64) (zFast, zSlow float64) {
	return zScore(priceStep, r.FastMeanPriceStep, r.FastVarPriceStep),
		zScore(priceStep, r.SlowMeanPriceStep, r.SlowVarPriceStep)
}

func (r *RollingStats) IsWarm() bool {
	return r.ObservationCount >= r.MinObservations
}

func (r *RollingStats) GapFlag(intertickMs float64) int {
	if r.FastMeanIntertick > 0 && intertickMs > r.GapMultiplier*r.FastMeanIntertick {
		return 1
	}
	return 0
}

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
