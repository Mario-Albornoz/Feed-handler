package model

import (
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"
)

// TestRegistryGetOrCreate_Basic verifies basic functionality
func TestRegistryGetOrCreate_Basic(t *testing.T) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)

	key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: "AAPL"}

	// First call should create
	state1 := registry.GetOrCreate(key)
	if state1 == nil {
		t.Fatal("GetOrCreate should return non-nil state")
	}

	// Second call should return same instance
	state2 := registry.GetOrCreate(key)
	if state1 != state2 {
		t.Error("GetOrCreate should return same instance for same key")
	}

	// Different key should create different instance
	key2 := InstrumentKey{Source: "NASDAQ", InstrumentIdentifier: "MSFT"}
	state3 := registry.GetOrCreate(key2)
	if state1 == state3 {
		t.Error("Different keys should return different instances")
	}
}

// TestRegistryGetOrCreate_Concurrent verifies thread safety
func TestRegistryGetOrCreate_Concurrent(t *testing.T) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)
	key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: "AAPL"}

	const numGoroutines = 100
	var wg sync.WaitGroup
	states := make([]*InstrumentState, numGoroutines)

	// Launch many goroutines trying to GetOrCreate the same key
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			states[index] = registry.GetOrCreate(key)
		}(i)
	}

	wg.Wait()

	// All goroutines should get the exact same instance
	firstState := states[0]
	for i := 1; i < numGoroutines; i++ {
		if states[i] != firstState {
			t.Errorf("Goroutine %d got different instance", i)
		}
	}

	// Registry should contain exactly one entry
	all := registry.All()
	if len(all) != 1 {
		t.Errorf("Expected 1 entry in registry, got %d", len(all))
	}
}

// TestRegistrySaveLoadRoundTrip verifies persistence preserves state
func TestRegistrySaveLoadRoundTrip(t *testing.T) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)

	// Create and warm up several instruments
	keys := []InstrumentKey{
		{Source: "NYSE", InstrumentIdentifier: "AAPL"},
		{Source: "NYSE", InstrumentIdentifier: "GOOGL"},
		{Source: "NASDAQ", InstrumentIdentifier: "MSFT"},
	}

	for _, key := range keys {
		state := registry.GetOrCreate(key)

		// Warm up with observations
		for i := 0; i < 100; i++ {
			state.StatsBySession[Open].Update(10.0, 0.01)
			state.AllSessionStats.Update(10.0, 0.01)
		}

		state.LastTickTime = time.Now()
		state.PrevLastTradedPrice = 123.45
	}

	// Capture original values for verification
	originalStates := make(map[InstrumentKey]struct {
		slowMean            float64
		fastMean            float64
		obsCount            int64
		lastTickTime        time.Time
		prevLastTradedPrice float64
	})

	for _, key := range keys {
		state := registry.GetOrCreate(key)
		originalStates[key] = struct {
			slowMean            float64
			fastMean            float64
			obsCount            int64
			lastTickTime        time.Time
			prevLastTradedPrice float64
		}{
			slowMean:            state.StatsBySession[Open].SlowMeanIntertick,
			fastMean:            state.StatsBySession[Open].FastMeanIntertick,
			obsCount:            state.StatsBySession[Open].ObservationCount,
			lastTickTime:        state.LastTickTime,
			prevLastTradedPrice: state.PrevLastTradedPrice,
		}
	}

	// Save to file
	testFile := "test_registry_snapshot.gob"
	defer os.Remove(testFile)

	err := registry.Save(testFile)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Create new registry and load
	newRegistry := NewInstrumentRegistry(60, 1000, 0.5)
	err = newRegistry.Load(testFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify all states preserved
	for _, key := range keys {
		state := newRegistry.GetOrCreate(key)
		original := originalStates[key]

		// Check EMA values (within float tolerance)
		if math.Abs(state.StatsBySession[Open].SlowMeanIntertick-original.slowMean) > 1e-9 {
			t.Errorf("SlowMean not preserved for %+v: got %f, want %f",
				key, state.StatsBySession[Open].SlowMeanIntertick, original.slowMean)
		}

		if math.Abs(state.StatsBySession[Open].FastMeanIntertick-original.fastMean) > 1e-9 {
			t.Errorf("FastMean not preserved for %+v: got %f, want %f",
				key, state.StatsBySession[Open].FastMeanIntertick, original.fastMean)
		}

		// Check observation count
		if state.StatsBySession[Open].ObservationCount != original.obsCount {
			t.Errorf("ObservationCount not preserved for %+v: got %d, want %d",
				key, state.StatsBySession[Open].ObservationCount, original.obsCount)
		}

		// Check LastTickTime (within 1 second tolerance due to gob precision)
		if state.LastTickTime.Sub(original.lastTickTime).Abs() > time.Second {
			t.Errorf("LastTickTime not preserved for %+v: got %v, want %v",
				key, state.LastTickTime, original.lastTickTime)
		}

		// Check PrevLastTradedPrice
		if math.Abs(state.PrevLastTradedPrice-original.prevLastTradedPrice) > 1e-9 {
			t.Errorf("PrevLastTradedPrice not preserved for %+v: got %f, want %f",
				key, state.PrevLastTradedPrice, original.prevLastTradedPrice)
		}
	}
}

// TestRegistryAll_Snapshot verifies All() returns current state safely
func TestRegistryAll_Snapshot(t *testing.T) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)

	// Pre-populate with some instruments
	for i := 0; i < 10; i++ {
		key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: fmt.Sprintf("TICK%d", i)}
		registry.GetOrCreate(key)
	}

	// Get reference to registry state
	snapshot := registry.All()
	if len(snapshot) != 10 {
		t.Errorf("Expected 10 instruments, got %d", len(snapshot))
	}

	// Add more instruments
	for i := 10; i < 20; i++ {
		key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: fmt.Sprintf("TICK%d", i)}
		registry.GetOrCreate(key)
	}

	// All() should now show new size
	newSnapshot := registry.All()
	if len(newSnapshot) != 20 {
		t.Errorf("Registry should have 20 instruments, got %d", len(newSnapshot))
	}

	// Verify we can safely iterate over snapshot
	count := 0
	for key, state := range newSnapshot {
		if state == nil {
			t.Errorf("Found nil state for key %+v", key)
		}
		count++
	}
	if count != 20 {
		t.Errorf("Iterated over %d instruments, expected 20", count)
	}
}

// TestRegistryAll_ConcurrentAccess verifies All() is safe during concurrent writes
func TestRegistryAll_ConcurrentAccess(t *testing.T) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)

	var wg sync.WaitGroup

	// Simulate consumer goroutine adding instruments
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			key := InstrumentKey{
				Source:               "NYSE",
				InstrumentIdentifier: fmt.Sprintf("TICK%d", i),
			}
			state := registry.GetOrCreate(key)
			// Simulate some work
			state.LastTickTime = time.Now()
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Simulate silence detector reading snapshots
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			snapshot := registry.All()
			// Access states (shouldn't crash or race)
			for _, state := range snapshot {
				_ = state.LastTickTime
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	// If we get here without panicking, test passed
}

// TestRegistryLoad_MissingFile verifies graceful handling of missing file
func TestRegistryLoad_MissingFile(t *testing.T) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)

	err := registry.Load("nonexistent_file_12345.gob")
	if err == nil {
		t.Error("Load should return error for missing file")
	}

	// Registry should still be usable
	key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: "AAPL"}
	state := registry.GetOrCreate(key)
	if state == nil {
		t.Error("Registry should be usable after failed load")
	}
}

// TestRegistryLoad_CorruptFile verifies handling of corrupt data
func TestRegistryLoad_CorruptFile(t *testing.T) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)

	// Create a file with corrupt data
	testFile := "test_corrupt.gob"
	defer os.Remove(testFile)

	err := os.WriteFile(testFile, []byte("this is not gob data!"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Load should return error
	err = registry.Load(testFile)
	if err == nil {
		t.Error("Load should return error for corrupt file")
	}

	// Registry should still be usable (may be empty or partially loaded)
	key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: "AAPL"}
	state := registry.GetOrCreate(key)
	if state == nil {
		t.Error("Registry should be usable after failed load")
	}
}

// TestRegistrySave_AllSessionsStats verifies both session and fallback stats are saved
func TestRegistrySave_AllSessionsStats(t *testing.T) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)
	key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: "AAPL"}
	state := registry.GetOrCreate(key)

	// Update both session-specific and fallback stats
	for i := 0; i < 100; i++ {
		state.StatsBySession[Open].Update(10.0, 0.01)
		state.AllSessionStats.Update(15.0, 0.02)
	}

	originalSessionMean := state.StatsBySession[Open].SlowMeanIntertick
	originalFallbackMean := state.AllSessionStats.SlowMeanIntertick

	// Save and load
	testFile := "test_allsessions.gob"
	defer os.Remove(testFile)

	err := registry.Save(testFile)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	newRegistry := NewInstrumentRegistry(60, 1000, 0.5)
	err = newRegistry.Load(testFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	loadedState := newRegistry.GetOrCreate(key)

	// Verify session-specific stats preserved
	if math.Abs(loadedState.StatsBySession[Open].SlowMeanIntertick-originalSessionMean) > 1e-9 {
		t.Errorf("Session stats not preserved: got %f, want %f",
			loadedState.StatsBySession[Open].SlowMeanIntertick, originalSessionMean)
	}

	// Verify fallback stats preserved
	if math.Abs(loadedState.AllSessionStats.SlowMeanIntertick-originalFallbackMean) > 1e-9 {
		t.Errorf("Fallback stats not preserved: got %f, want %f",
			loadedState.AllSessionStats.SlowMeanIntertick, originalFallbackMean)
	}
}

// BenchmarkRegistryGetOrCreate_Existing measures hot path performance
func BenchmarkRegistryGetOrCreate_Existing(b *testing.B) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)
	key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: "AAPL"}

	// Pre-create the instrument (hot path case)
	registry.GetOrCreate(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.GetOrCreate(key)
	}
}

// BenchmarkRegistryGetOrCreate_New measures cold path performance
func BenchmarkRegistryGetOrCreate_New(b *testing.B) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := InstrumentKey{Source: "NYSE", InstrumentIdentifier: fmt.Sprintf("TICK%d", i)}
		registry.GetOrCreate(key)
	}
}

// BenchmarkRegistryGetOrCreate_Concurrent measures concurrent read performance
func BenchmarkRegistryGetOrCreate_Concurrent(b *testing.B) {
	registry := NewInstrumentRegistry(60, 1000, 0.5)

	// Pre-create 100 instruments
	keys := make([]InstrumentKey, 100)
	for i := 0; i < 100; i++ {
		keys[i] = InstrumentKey{Source: "NYSE", InstrumentIdentifier: fmt.Sprintf("TICK%d", i)}
		registry.GetOrCreate(keys[i])
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Round-robin through keys
			key := keys[i%len(keys)]
			registry.GetOrCreate(key)
			i++
		}
	})
}
