package stats

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestGapQuantileMatchesTheEmpiricalQuantile(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	g := NewGapQuantile(1e9) // no decay
	var gaps []float64
	for i := 0; i < 20000; i++ {
		// heavy-tailed, like the real gaps: mostly small, occasionally long
		gap := math.Floor(math.Exp(rng.NormFloat64()*2 + 5))
		gaps = append(gaps, gap)
		g.Observe(gap)
	}
	sort.Float64s(gaps)

	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		exact := gaps[int(q*float64(len(gaps)))-1]
		got := g.Quantile(q)
		// the estimate is the upper edge of a bucket about 19% wide
		if got < exact*0.99 || got > exact*1.25+1 {
			t.Errorf("q=%v: histogram says %.1f ms, exact is %.1f ms", q, got, exact)
		}
	}
}

func TestGapQuantileIsEmptyAtStart(t *testing.T) {
	g := NewGapQuantile(100)
	if g.Quantile(0.999) != 0 || g.N != 0 {
		t.Errorf("an empty histogram has no quantile: %v (N=%d)", g.Quantile(0.999), g.N)
	}
}

func TestGapQuantileTreatsNegativeGapsAsZero(t *testing.T) {
	g := NewGapQuantile(100)
	for i := 0; i < 100; i++ {
		g.Observe(-500)
	}
	if q := g.Quantile(0.999); q > 1 {
		t.Errorf("negative gaps count as 0, got a quantile of %v ms", q)
	}
}

// After the window fills the estimate follows the instrument's recent gaps.
func TestGapQuantileForgetsOldGaps(t *testing.T) {
	g := NewGapQuantile(200)
	for i := 0; i < 500; i++ {
		g.Observe(60000) // an early, slow regime
	}
	for i := 0; i < 5000; i++ {
		g.Observe(1000) // the instrument speeds up
	}
	if q := g.Quantile(0.99); q > 2000 {
		t.Errorf("the slow regime should have decayed away, 99th percentile is %.0f ms", q)
	}
}

func TestGapQuantileIsExactBeforeTheWindowFills(t *testing.T) {
	g := NewGapQuantile(1000)
	for i := 0; i < 900; i++ {
		g.Observe(1000)
	}
	g.Observe(60000)
	if math.Abs(g.Total-901) > 1e-9 {
		t.Errorf("no decay before the window fills: total %.3f, want 901", g.Total)
	}
}
