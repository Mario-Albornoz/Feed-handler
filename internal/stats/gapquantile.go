package stats

import "math"

const (
	gapBucketsPerOctave = 4
	gapBuckets          = 160 // 4 per octave up to 2^40 ms
)

// GapQuantile estimates high quantiles of an instrument's inter-message gaps from a
// log-spaced histogram (four buckets per octave, so about 19% resolution).
//
// It is what the silence detector compares a gap against: an instrument is quiet when
// its gap exceeds a high quantile of its own past gaps. Inter-arrival times here are
// bursty and heavy-tailed, so a multiple of the mean interval fires on ordinary gaps
// (measured: 34-47 alerts per 1000 messages on a clean feed), while a quantile of the
// instrument's own distribution adapts to it (about 1 per 1000 at the 99.9th).
//
// The counts are exact until Window observations have been seen, then decay
// exponentially so the estimate follows the instrument's recent behaviour. All fields
// are exported so the registry snapshot (gob) keeps them.
type GapQuantile struct {
	Counts [gapBuckets]float64
	Total  float64
	N      int64   // observations so far
	Window float64 // decay starts after this many observations
}

func NewGapQuantile(window float64) GapQuantile {
	return GapQuantile{Window: window}
}

func gapBucket(gapMs float64) int {
	b := int(gapBucketsPerOctave * math.Log2(gapMs+1))
	if b < 0 {
		return 0
	}
	if b >= gapBuckets {
		return gapBuckets - 1
	}
	return b
}

// gapUpperEdge is the largest gap (ms) that falls in bucket b.
func gapUpperEdge(b int) float64 {
	return math.Exp2(float64(b+1)/gapBucketsPerOctave) - 1
}

// Observe records one gap in milliseconds (negative gaps count as 0).
func (g *GapQuantile) Observe(gapMs float64) {
	g.N++
	if g.Window > 0 && float64(g.N) > g.Window {
		decay := 1 - 1/g.Window
		for i := range g.Counts {
			g.Counts[i] *= decay
		}
		g.Total *= decay
	}
	g.Counts[gapBucket(math.Max(gapMs, 0))]++
	g.Total++
}

// Quantile returns the upper edge (ms) of the bucket holding the q-quantile, or 0 if
// nothing has been observed.
func (g *GapQuantile) Quantile(q float64) float64 {
	if g.Total <= 0 {
		return 0
	}
	target := q * g.Total
	cum := 0.0
	for b := 0; b < gapBuckets; b++ {
		cum += g.Counts[b]
		if cum >= target {
			return gapUpperEdge(b)
		}
	}
	return gapUpperEdge(gapBuckets - 1)
}
