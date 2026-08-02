package stats

import (
	"math"
	"testing"
)

// TestWelfordConvergence verifies that the EMA mean converges
// to the true mean after sufficient observations.
func TestWelfordConvergence(t *testing.T) {
	rs := NewRollingStats(60, 14400, 1.0, 0.5)
	// Feed 500 ticks with intertick=10ms, spread=0.04, step=0.01, vol=100
	for i := 0; i < 500; i++ {
		rs.Update(10.0, 0.04, 0.01, 100.0)
	}
	// Fast mean should be close to 10ms
	if math.Abs(rs.FastMeanIntertick-10.0) > 1.0 {
		t.Errorf("fast mean intertick: got %.4f, want ~10.0", rs.FastMeanIntertick)
	}
}

// TestZScoreNormalTick verifies that a normal tick produces a z-score near zero.
func TestZScoreNormalTick(t *testing.T) {
	rs := NewRollingStats(60, 14400, 1.0, 0.5)
	for i := 0; i < 200; i++ {
		rs.Update(10.0, 0.04, 0.01, 100.0)
	}
	zfi, _, _, _, _, _, _, _ := rs.ZScores(10.0, 0.04, 0.01, 100.0)
	if math.Abs(zfi) > 1.0 {
		t.Errorf("z_intertick_fast for normal tick: got %.4f, want near 0", zfi)
	}
}

// TestZScoreAnomalousTick verifies that an extreme tick produces a high z-score.
func TestZScoreAnomalousTick(t *testing.T) {
	rs := NewRollingStats(60, 14400, 1.0, 0.5)
	for i := 0; i < 200; i++ {
		rs.Update(10.0, 0.04, 0.01, 100.0)
	}
	// Feed a tick with 100x normal intertick interval
	zfi, _, _, _, _, _, _, _ := rs.ZScores(1000.0, 0.04, 0.01, 100.0)
	if zfi < 5.0 {
		t.Errorf("z_intertick_fast for anomalous tick: got %.4f, want > 5.0", zfi)
	}
}

// TestCUSUMAccumulates verifies CUSUM rises during sustained drift.
func TestCUSUMAccumulates(t *testing.T) {
	rs := NewRollingStats(60, 14400, 1.0, 0.5)
	// Warm up with normal data
	for i := 0; i < 200; i++ {
		rs.Update(10.0, 0.04, 0.01, 100.0)
	}
	initialCusum := rs.CusumSpread
	// Feed gradually widening spread
	for i := 0; i < 100; i++ {
		drift := 0.04 + float64(i)*0.002
		rs.Update(10.0, drift, 0.01, 100.0)
	}
	if rs.CusumSpread <= initialCusum {
		t.Errorf("CUSUM did not accumulate during drift: initial=%.4f final=%.4f",
			initialCusum, rs.CusumSpread)
	}
}

// TestCUSUMResets verifies CUSUM resets when signal returns to normal.
func TestCUSUMResets(t *testing.T) {
	rs := NewRollingStats(60, 14400, 1.0, 0.5)
	for i := 0; i < 200; i++ {
		rs.Update(10.0, 0.04, 0.01, 100.0)
	}
	// Drift up
	for i := 0; i < 100; i++ {
		rs.Update(10.0, 0.04+float64(i)*0.003, 0.01, 100.0)
	}
	// Return to normal
	for i := 0; i < 300; i++ {
		rs.Update(10.0, 0.04, 0.01, 100.0)
	}
	if rs.CusumSpread > 1.0 {
		t.Errorf("CUSUM did not reset after recovery: got %.4f", rs.CusumSpread)
	}
}

// TestWarmupFlag verifies warmup flag clears after MinObservations ticks.
func TestWarmupFlag(t *testing.T) {
	rs := NewRollingStats(60, 14400, 1.0, 0.5)
	if rs.IsWarm() {
		t.Error("should not be warm at start")
	}
	for i := 0; i < int(rs.MinObservations); i++ {
		rs.Update(10.0, 0.04, 0.01, 100.0)
	}
	if !rs.IsWarm() {
		t.Error("should be warm after MinObservations ticks")
	}
}

// TestGapFlag verifies gap flag fires when intertick exceeds multiplier * mean.
func TestGapFlag(t *testing.T) {
	rs := NewRollingStats(60, 14400, 1.0, 0.5)
	for i := 0; i < 100; i++ {
		rs.Update(10.0, 0.04, 0.01, 100.0)
	}
	// 10ms mean * 5x multiplier = 50ms threshold
	if rs.GapFlag(40.0) != 0 {
		t.Error("gap flag should not fire at 40ms with 10ms mean and 5x multiplier")
	}
	if rs.GapFlag(60.0) != 1 {
		t.Error("gap flag should fire at 60ms with 10ms mean and 5x multiplier")
	}
}
