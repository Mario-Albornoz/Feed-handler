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

// UpdateIntertick records one inter-tick observation (milliseconds since the
// instrument's previous message). It is called for every message, trade or quote.
//
// The averages are exponential with the configured window, but during warm-up they
// are plain running averages: the weight of observation n is max(alpha, 1/n). An
// exponential average that starts at zero needs several windows to converge (with a
// n-tick window it is under 1% of the true mean after 50 observations), which
// would distort every slow-timescale z-score and silence threshold early in an
// instrument's history. With weight 1/n the mean and variance are exactly the sample
// mean and variance until 1/n falls below alpha, then the exponential takes over.
func (r *RollingStats) UpdateIntertick(intertick float64) {
	r.ObservationCount++
	warmup := 1.0 / float64(r.ObservationCount)
	fastAlpha := math.Max(r.FastAlpha, warmup)
	slowAlpha := math.Max(r.SlowAlpha, warmup)

	r.FastMeanIntertick, r.FastVarIntertick = updateEMA(r.FastMeanIntertick, r.FastVarIntertick, intertick, fastAlpha)
	r.SlowMeanIntertick, r.SlowVarIntertick = updateEMA(r.SlowMeanIntertick, r.SlowVarIntertick, intertick, slowAlpha)

	zSlowIntertick := zScore(intertick, r.SlowMeanIntertick, r.SlowVarIntertick)
	r.CusumIntertick = math.Max(0, r.CusumIntertick+zSlowIntertick-r.CusumSlack)
}

// UpdatePriceStep records one price-step observation: the absolute change between two
func (r *RollingStats) UpdatePriceStep(priceStep float64) {
	r.PriceObservationCount++
	warmup := 1.0 / float64(r.PriceObservationCount)
	fastAlpha := math.Max(r.FastAlpha, warmup)
	slowAlpha := math.Max(r.SlowAlpha, warmup)

	r.FastMeanPriceStep, r.FastVarPriceStep = updateEMA(r.FastMeanPriceStep, r.FastVarPriceStep, priceStep, fastAlpha)
	r.SlowMeanPriceStep, r.SlowVarPriceStep = updateEMA(r.SlowMeanPriceStep, r.SlowVarPriceStep, priceStep, slowAlpha)

	zSlowPriceStep := zScore(priceStep, r.SlowMeanPriceStep, r.SlowVarPriceStep)
	r.CusumPriceStep = math.Max(0, r.CusumPriceStep+zSlowPriceStep-r.CusumSlack)
}

// Update records one message that has both an inter-tick interval and a price step.
func (r *RollingStats) Update(intertick, priceStep float64) {
	r.UpdateIntertick(intertick)
	r.UpdatePriceStep(priceStep)
}

// IntertickZScores should be called after UpdateIntertick().
func (r *RollingStats) IntertickZScores(intertick float64) (zFast, zSlow float64) {
	return zScore(intertick, r.FastMeanIntertick, r.FastVarIntertick),
		zScore(intertick, r.SlowMeanIntertick, r.SlowVarIntertick)
}

// PriceStepZScores should be called after UpdatePriceStep().
func (r *RollingStats) PriceStepZScores(priceStep float64) (zFast, zSlow float64) {
	return zScore(priceStep, r.FastMeanPriceStep, r.FastVarPriceStep),
		zScore(priceStep, r.SlowMeanPriceStep, r.SlowVarPriceStep)
}

// ZScores should be called after Update().
func (r *RollingStats) ZScores(intertick, priceStep float64) (
	zFastIntertick, zFastPriceStep,
	zSlowIntertick, zSlowPriceStep float64,
) {
	zFastIntertick, zSlowIntertick = r.IntertickZScores(intertick)
	zFastPriceStep, zSlowPriceStep = r.PriceStepZScores(priceStep)
	return
}

// IsWarm returns true when enough observations have been collected
// for rolling statistics to be reliable.
func (r *RollingStats) IsWarm() bool {
	return r.ObservationCount >= r.MinObservations
}

// PriceIsWarm is IsWarm for the price-step statistics.
func (r *RollingStats) PriceIsWarm() bool {
	return r.PriceObservationCount >= r.MinPriceObservations
}

// GapFlag returns 1 if the current intertick interval exceeds
// GapMultiplier times the fast rolling mean. 0 otherwise.
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
