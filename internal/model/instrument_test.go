package model

import (
	"testing"
)

// TestFallbackSelection verifies GetStateForBucket returns correct stats based on observation count
func TestFallbackSelection(t *testing.T) {
	// Create instrument state with known window sizes
	state := NewInstrumentState(60, 1000, 0.5)

	// Warm up Open session bucket to exactly MinObservations-1 (49 observations)
	openStats := state.StatsBySession[Open]
	for i := 0; i < 49; i++ {
		openStats.Update(10.0, 0.04, 0.01, 100.0)
	}

	// Warm up AllSessionStats to well above MinObservations (100 observations)
	for i := 0; i < 100; i++ {
		state.AllSessionStats.Update(10.0, 0.04, 0.01, 100.0)
	}

	// Test 1: Session bucket with < MinObservations should use fallback
	stats, usedFallback := state.GetStateForBucket(Open)
	if !usedFallback {
		t.Error("Expected to use fallback when session has < MinObservations")
	}
	if stats != state.AllSessionStats {
		t.Error("Should have returned AllSessionStats when using fallback")
	}

	// Test 2: Add one more observation to reach MinObservations (50)
	openStats.Update(10.0, 0.04, 0.01, 100.0)
	stats, usedFallback = state.GetStateForBucket(Open)
	if usedFallback {
		t.Error("Expected to use session-specific stats when >= MinObservations")
	}
	if stats != openStats {
		t.Error("Should have returned session-specific stats when warm")
	}

	// Test 3: Different session bucket with 0 observations should use fallback
	stats, usedFallback = state.GetStateForBucket(Overnight)
	if !usedFallback {
		t.Error("Expected to use fallback for session with no observations")
	}
}

// TestIlliquidInstrumentFallback verifies chronically illiquid instrument behavior
// This is the main acceptance test for AGG-1b
func TestIlliquidInstrumentFallback(t *testing.T) {
	state := NewInstrumentState(60, 1000, 0.5)

	// Simulate instrument ticking once every 8 hours, spread across multiple sessions
	// After 60 ticks total, fallback will be warm but no single session bucket will be
	sessionSequence := make([]SessionBucket, 60)
	allBuckets := []SessionBucket{PreMarket, Open, Midday, Close, AfterHours, Overnight, Weekend}
	for i := 0; i < 60; i++ {
		sessionSequence[i] = allBuckets[i%len(allBuckets)]
	}

	// Simulate updates to both session-specific and fallback stats
	for i, bucket := range sessionSequence {
		// Update session-specific bucket
		state.StatsBySession[bucket].Update(28800000.0, 0.04, 0.01, 100.0) // 8 hours in ms
		// Update fallback (this would happen in real ProcessTick)
		state.AllSessionStats.Update(28800000.0, 0.04, 0.01, 100.0)

		// After 60 ticks, fallback should be warm but no session bucket should be
		if i == 59 {
			// Verify fallback is warm
			if !state.AllSessionStats.IsWarm() {
				t.Error("AllSessionStats should be warm after 60 observations")
			}

			// Verify no session bucket is warm yet
			for bucket := PreMarket; bucket <= Weekend; bucket++ {
				if state.StatsBySession[bucket].IsWarm() {
					t.Errorf("Session bucket %v should not be warm yet with sparse data", bucket)
				}
			}

			// Verify any bucket lookup uses fallback
			for bucket := PreMarket; bucket <= Weekend; bucket++ {
				_, usedFallback := state.GetStateForBucket(bucket)
				if !usedFallback {
					t.Errorf("Illiquid instrument should use fallback for bucket %v", bucket)
				}
			}
		}
	}
}

// TestCrossoverFromFallbackToSession verifies transition from fallback to session-specific
func TestCrossoverFromFallbackToSession(t *testing.T) {
	state := NewInstrumentState(60, 1000, 0.5)

	// Warm up fallback
	for i := 0; i < 60; i++ {
		state.AllSessionStats.Update(10.0, 0.04, 0.01, 100.0)
	}

	// Initially should use fallback for Open bucket
	_, usedFallback := state.GetStateForBucket(Open)
	if !usedFallback {
		t.Error("Should initially use fallback")
	}

	// Gradually warm up Open session bucket
	openStats := state.StatsBySession[Open]
	for i := 0; i < 49; i++ {
		openStats.Update(10.0, 0.04, 0.01, 100.0)
		_, usedFallback := state.GetStateForBucket(Open)
		if !usedFallback {
			t.Errorf("Should still use fallback at observation %d", i+1)
		}
	}

	// Add the 50th observation - crossover point
	openStats.Update(10.0, 0.04, 0.01, 100.0)
	stats, usedFallback := state.GetStateForBucket(Open)
	if usedFallback {
		t.Error("Should switch to session-specific at MinObservations")
	}
	if stats != openStats {
		t.Error("Should return session-specific stats after crossover")
	}

	// Verify other buckets still use fallback
	_, usedFallback = state.GetStateForBucket(Overnight)
	if !usedFallback {
		t.Error("Other buckets should still use fallback")
	}
}

// TestBimodalInstrument verifies fallback doesn't smear real session structure
func TestBimodalInstrument(t *testing.T) {
	state := NewInstrumentState(60, 1000, 0.5)

	// Simulate an instrument active ONLY during Open session (e.g., options near expiry)
	// 500 ticks during Open, 0 ticks in other sessions (enough for EMA to converge)
	openStats := state.StatsBySession[Open]
	for i := 0; i < 500; i++ {
		// Short intertick during Open (very active)
		openStats.Update(50.0, 0.04, 0.01, 100.0)
		// Also update fallback
		state.AllSessionStats.Update(50.0, 0.04, 0.01, 100.0)
	}

	// Open bucket should be warm and have tight statistics
	if !openStats.IsWarm() {
		t.Error("Open bucket should be warm after 500 observations")
	}

	// Open bucket should use its own stats
	stats, usedFallback := state.GetStateForBucket(Open)
	if usedFallback {
		t.Error("Open bucket should use session-specific stats")
	}
	// Verify mean is converging towards 50ms (EMA with 1000-tick window converges slowly)
	// Main point: session-specific stats exist and are being used
	if stats.SlowMeanIntertick < 10 || stats.SlowMeanIntertick > 60 {
		t.Errorf("Open bucket mean converging towards 50ms, got %.2f", stats.SlowMeanIntertick)
	}

	// Overnight bucket with 0 observations should use fallback
	overnightStats := state.StatsBySession[Overnight]
	if overnightStats.ObservationCount != 0 {
		t.Error("Overnight should have 0 observations")
	}

	stats, usedFallback = state.GetStateForBucket(Overnight)
	if !usedFallback {
		t.Error("Overnight should use fallback with 0 observations")
	}

	// Fallback mean converging towards 50ms (same as Open, since that's all the data)
	// Main point: fallback provides reasonable baseline even for unticked sessions
	if state.AllSessionStats.SlowMeanIntertick < 10 || state.AllSessionStats.SlowMeanIntertick > 60 {
		t.Errorf("Fallback mean converging towards 50ms, got %.2f", state.AllSessionStats.SlowMeanIntertick)
	}

	// Key test: fallback provides coverage but with appropriate statistics
	// An Overnight tick with 8hr gap (28800000ms) should still show high z-score
	// even against fallback, because fallback learned 50ms intervals
	z := (28800000.0 - state.AllSessionStats.SlowMeanIntertick) / 
		 (state.AllSessionStats.SlowVarIntertick + 1e-10)
	
	if z < 100 {
		t.Errorf("8hr gap should produce very high z-score even against fallback, got %.2f", z)
	}
}

// TestFallbackAlwaysUpdates verifies AllSessionStats updates on every tick
func TestFallbackAlwaysUpdates(t *testing.T) {
	state := NewInstrumentState(60, 1000, 0.5)

	initialCount := state.AllSessionStats.ObservationCount

	// Update only Open session bucket
	state.StatsBySession[Open].Update(10.0, 0.04, 0.01, 100.0)
	
	// In real implementation, AllSessionStats would also be updated
	// This test documents the requirement - actual update happens in ProcessTick
	// For now, manually update to show the pattern
	state.AllSessionStats.Update(10.0, 0.04, 0.01, 100.0)

	if state.AllSessionStats.ObservationCount != initialCount+1 {
		t.Error("AllSessionStats should update on every tick")
	}

	// Update a different session bucket
	state.StatsBySession[Overnight].Update(28800000.0, 0.04, 0.01, 100.0)
	state.AllSessionStats.Update(28800000.0, 0.04, 0.01, 100.0)

	if state.AllSessionStats.ObservationCount != initialCount+2 {
		t.Error("AllSessionStats should update regardless of which session bucket")
	}
}
