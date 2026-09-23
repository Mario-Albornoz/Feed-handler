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

	Limits Limits
}

// Limits bound the z-scores. A z-score is taken against statistics that do not yet
// include the observation, so nothing caps it: the EMA variance shrinks geometrically
// during a run of identical values (half of all trade-to-trade price steps are 0), and
// the next ordinary value then scores in the millions. One such value pushes the CUSUM
// to a level its slack (0.5 a step) never works off.
type Limits struct {
	// IntertickStdFloorMs is the smallest standard deviation an inter-tick interval is
	// scored against: the rounding noise of the feature clock, resolution/sqrt(12).
	IntertickStdFloorMs float64
	// PriceStepStdFloorFrac is the same floor for price steps, as a fraction of the
	// previous traded price (the price grid is not known per instrument).
	PriceStepStdFloorFrac float64
	// CusumZClip bounds the z-score each observation adds to the CUSUM, so one extreme
	// value cannot hold the CUSUM up for days. 0 disables the clip.
	CusumZClip float64
	// WinsorZ winsorizes the statistics: once they are warm, an observation enters the
	// mean and variance clipped to mean +- WinsorZ standard deviations (the same floored
	// deviation it is scored against). Without it one outlier inflates the variance, and
	// the next anomaly on the same instrument is measured against that inflated variance
	// and looks ordinary (masking). The z-scores and the CUSUM still see the unclipped
	// value. 0 disables it.
	WinsorZ float64
	// ResetCusumDaily sets an instrument's CUSUMs to 0 on its first message of a new day,
	// so evidence accumulated in one session does not carry into the next.
	ResetCusumDaily bool
}

// NewLimits derives the floors from the resolution of the measurements: the timing
// clock (ms) and the price grid (basis points of the price).
func NewLimits(timingResolutionMs, priceResolutionBps, cusumZClip float64) Limits {
	return Limits{
		IntertickStdFloorMs:   timingResolutionMs / math.Sqrt(12),
		PriceStepStdFloorFrac: priceResolutionBps * 1e-4 / math.Sqrt(12),
		CusumZClip:            cusumZClip,
	}
}

// Defaults: whole-second update times, a 1 bp price grid, CUSUM steps within +-10,
// statistics winsorized at 10 standard deviations, CUSUMs reset every day.
//
// WinsorZ = 10 was chosen on the trades of 08 Nov with 1% simulated 2-5x spikes: every
// level tried (3, 5, 10) removed the masking (spikes after a spike scoring below 5 fell
// from 25% to 0%), and 10 changed normal traffic least (share of |z| > 5 on unmodified
// trades 0.02% without winsorizing, 0.15% at 10, 0.27% at 5, 0.55% at 3): a tighter clip
// also clips the heavy but normal tail of price steps and underestimates the variance.
const (
	DefaultTimingResolutionMs = 1000.0
	DefaultPriceResolutionBps = 1.0
	DefaultCusumZClip         = 10.0
	DefaultWinsorZ            = 10.0
)

func DefaultLimits() Limits {
	l := NewLimits(DefaultTimingResolutionMs, DefaultPriceResolutionBps, DefaultCusumZClip)
	l.WinsorZ = DefaultWinsorZ
	l.ResetCusumDaily = true
	return l
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

		Limits: DefaultLimits(),
	}
}

func (r *RollingStats) UpdateIntertick(intertick float64) {
	// The CUSUM takes the z-score against the statistics before this observation.
	_, zSlowIntertick := r.IntertickZScores(intertick)
	r.CusumIntertick = math.Max(0, r.CusumIntertick+r.clip(zSlowIntertick)-r.CusumSlack)

	// Winsorize only once warm: early estimates are too rough to clip against.
	warm := r.ObservationCount >= r.MinObservations
	floor := r.Limits.IntertickStdFloorMs
	xFast := r.winsorize(intertick, r.FastMeanIntertick, r.FastVarIntertick, floor, warm)
	xSlow := r.winsorize(intertick, r.SlowMeanIntertick, r.SlowVarIntertick, floor, warm)

	r.ObservationCount++
	// Weight max(alpha, 1/n): a plain running average until 1/n < alpha, so the averages
	// are not biased towards their zero start early in an instrument's history.
	warmup := 1.0 / float64(r.ObservationCount)
	fastAlpha := math.Max(r.FastAlpha, warmup)
	slowAlpha := math.Max(r.SlowAlpha, warmup)

	r.FastMeanIntertick, r.FastVarIntertick = updateEMA(r.FastMeanIntertick, r.FastVarIntertick, xFast, fastAlpha)
	r.SlowMeanIntertick, r.SlowVarIntertick = updateEMA(r.SlowMeanIntertick, r.SlowVarIntertick, xSlow, slowAlpha)
}

// UpdatePriceStep records one price step; price is the traded price the step is taken
// from, which scales the standard-deviation floor.
func (r *RollingStats) UpdatePriceStep(priceStep, price float64) {
	_, zSlowPriceStep := r.PriceStepZScores(priceStep, price)
	r.CusumPriceStep = math.Max(0, r.CusumPriceStep+r.clip(zSlowPriceStep)-r.CusumSlack)

	warm := r.PriceObservationCount >= r.MinPriceObservations
	floor := r.Limits.PriceStepStdFloorFrac * math.Abs(price)
	xFast := r.winsorize(priceStep, r.FastMeanPriceStep, r.FastVarPriceStep, floor, warm)
	xSlow := r.winsorize(priceStep, r.SlowMeanPriceStep, r.SlowVarPriceStep, floor, warm)

	r.PriceObservationCount++
	warmup := 1.0 / float64(r.PriceObservationCount)
	fastAlpha := math.Max(r.FastAlpha, warmup)
	slowAlpha := math.Max(r.SlowAlpha, warmup)

	r.FastMeanPriceStep, r.FastVarPriceStep = updateEMA(r.FastMeanPriceStep, r.FastVarPriceStep, xFast, fastAlpha)
	r.SlowMeanPriceStep, r.SlowVarPriceStep = updateEMA(r.SlowMeanPriceStep, r.SlowVarPriceStep, xSlow, slowAlpha)
}

// winsorize clips an observation to mean +- WinsorZ standard deviations of the given
// statistics before it updates them (see Limits.WinsorZ). The deviation is floored as in
// zScore, so a collapsed variance cannot freeze the statistics.
func (r *RollingStats) winsorize(x, mean, variance, stdFloor float64, warm bool) float64 {
	c := r.Limits.WinsorZ
	if !warm || c <= 0 {
		return x
	}
	std := math.Max(math.Sqrt(variance), stdFloor)
	if std < 1e-10 {
		return x
	}
	return math.Max(mean-c*std, math.Min(mean+c*std, x))
}

// ResetCusum clears both cumulative sums (see Limits.ResetCusumDaily).
func (r *RollingStats) ResetCusum() {
	r.CusumIntertick = 0
	r.CusumPriceStep = 0
}

// IntertickZScores scores an interval against the statistics before it is recorded.
// With no observation yet there is no baseline: neutral z-scores.
func (r *RollingStats) IntertickZScores(intertick float64) (zFast, zSlow float64) {
	if r.ObservationCount == 0 {
		return 0, 0
	}
	floor := r.Limits.IntertickStdFloorMs
	return zScore(intertick, r.FastMeanIntertick, r.FastVarIntertick, floor),
		zScore(intertick, r.SlowMeanIntertick, r.SlowVarIntertick, floor)
}

// PriceStepZScores is IntertickZScores for a price step taken from the given price.
func (r *RollingStats) PriceStepZScores(priceStep, price float64) (zFast, zSlow float64) {
	if r.PriceObservationCount == 0 {
		return 0, 0
	}
	floor := r.Limits.PriceStepStdFloorFrac * math.Abs(price)
	return zScore(priceStep, r.FastMeanPriceStep, r.FastVarPriceStep, floor),
		zScore(priceStep, r.SlowMeanPriceStep, r.SlowVarPriceStep, floor)
}

func (r *RollingStats) clip(z float64) float64 {
	if c := r.Limits.CusumZClip; c > 0 {
		return math.Max(-c, math.Min(c, z))
	}
	return z
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

func zScore(value, mean, variance, stdFloor float64) float64 {
	std := math.Max(math.Sqrt(variance), stdFloor)
	if std < 1e-10 {
		return 0
	}
	return (value - mean) / std
}
