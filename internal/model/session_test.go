package model

import (
	"testing"
	"time"
)

// TestResolveSessionBucket_BoundaryTests verifies exact boundary transitions
func TestResolveSessionBucket_BoundaryTests(t *testing.T) {
	// Setup NYSE exchange
	nyLoc, _ := time.LoadLocation("America/New_York")
	nyseHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,                 // 04:00
		MarketOpen:     9*time.Hour + 30*time.Minute,  // 09:30
		MiddayStart:    12 * time.Hour,                // 12:00
		CloseStart:     15*time.Hour + 30*time.Minute, // 15:30
		MarketClose:    16 * time.Hour,                // 16:00
		AfterHoursEnd:  20 * time.Hour,                // 20:00
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	exchanges := map[string]*ExchangeHours{"NYSE": nyseHours}
	resolver := &SessionResolver{exchanges: exchanges}

	tests := []struct {
		name     string
		time     string
		expected SessionBucket
	}{
		// Boundary: PreMarket → Open at 09:30:00
		{"09:29:59 is PreMarket", "2026-08-04T13:29:59Z", PreMarket}, // Monday
		{"09:30:00 is Open", "2026-08-04T13:30:00Z", Open},
		{"09:30:01 is Open", "2026-08-04T13:30:01Z", Open},

		// Boundary: Open → Midday at 12:00:00
		{"11:59:59 is Open", "2026-08-04T15:59:59Z", Open},
		{"12:00:00 is Midday", "2026-08-04T16:00:00Z", Midday},
		{"12:00:01 is Midday", "2026-08-04T16:00:01Z", Midday},

		// Boundary: Midday → Close at 15:30:00
		{"15:29:59 is Midday", "2026-08-04T19:29:59Z", Midday},
		{"15:30:00 is Close", "2026-08-04T19:30:00Z", Close},
		{"15:30:01 is Close", "2026-08-04T19:30:01Z", Close},

		// Boundary: Close → AfterHours at 16:00:00
		{"15:59:59 is Close", "2026-08-04T19:59:59Z", Close},
		{"16:00:00 is AfterHours", "2026-08-04T20:00:00Z", AfterHours},
		{"16:00:01 is AfterHours", "2026-08-04T20:00:01Z", AfterHours},

		// Boundary: AfterHours → Overnight at 20:00:00
		{"19:59:59 is AfterHours", "2026-08-04T23:59:59Z", AfterHours},
		{"20:00:00 is Overnight", "2026-08-05T00:00:00Z", Overnight},
		{"20:00:01 is Overnight", "2026-08-05T00:00:01Z", Overnight},

		// Boundary: Overnight → PreMarket at 04:00:00
		{"03:59:59 is Overnight", "2026-08-05T07:59:59Z", Overnight},
		{"04:00:00 is PreMarket", "2026-08-05T08:00:00Z", PreMarket},
		{"04:00:01 is PreMarket", "2026-08-05T08:00:01Z", PreMarket},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := time.Parse(time.RFC3339, tt.time)
			if err != nil {
				t.Fatalf("failed to parse time: %v", err)
			}

			bucket, err := resolver.ResolveSessionBucket(ts, "NYSE")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if bucket != tt.expected {
				t.Errorf("got %v, want %v", bucket, tt.expected)
			}
		})
	}
}

// TestResolveSessionBucket_Weekend verifies weekend detection
func TestResolveSessionBucket_Weekend(t *testing.T) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	nyseHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	exchanges := map[string]*ExchangeHours{"NYSE": nyseHours}
	resolver := &SessionResolver{exchanges: exchanges}

	tests := []struct {
		name     string
		time     string
		expected SessionBucket
	}{
		// August 1, 2026 is Saturday
		{"Saturday 10am is Weekend", "2026-08-01T14:00:00Z", Weekend},
		{"Saturday midnight is Weekend", "2026-08-01T04:00:00Z", Weekend},

		// August 2, 2026 is Sunday
		{"Sunday 10am is Weekend", "2026-08-02T14:00:00Z", Weekend},
		{"Sunday evening is Weekend", "2026-08-02T23:00:00Z", Weekend},

		// August 3, 2026 is Monday (should NOT be weekend during market hours)
		{"Monday 10am is Open", "2026-08-03T14:00:00Z", Open},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := time.Parse(time.RFC3339, tt.time)
			if err != nil {
				t.Fatalf("failed to parse time: %v", err)
			}

			bucket, err := resolver.ResolveSessionBucket(ts, "NYSE")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if bucket != tt.expected {
				t.Errorf("got %v, want %v", bucket, tt.expected)
			}
		})
	}
}

// TestResolveSessionBucket_MultipleExchanges verifies different timezones
func TestResolveSessionBucket_MultipleExchanges(t *testing.T) {
	// NYSE - Eastern Time
	nyLoc, _ := time.LoadLocation("America/New_York")
	nyseHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	// NASDAQ - Same as NYSE (Eastern Time)
	nasdaqHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	exchanges := map[string]*ExchangeHours{
		"NYSE":   nyseHours,
		"NASDAQ": nasdaqHours,
	}
	resolver := &SessionResolver{exchanges: exchanges}

	// Same UTC time should resolve to same bucket for same timezone exchanges
	utcTime, _ := time.Parse(time.RFC3339, "2026-08-04T14:00:00Z") // 10am ET

	nyseBucket, err := resolver.ResolveSessionBucket(utcTime, "NYSE")
	if err != nil {
		t.Fatalf("NYSE error: %v", err)
	}

	nasdaqBucket, err := resolver.ResolveSessionBucket(utcTime, "NASDAQ")
	if err != nil {
		t.Fatalf("NASDAQ error: %v", err)
	}

	if nyseBucket != Open {
		t.Errorf("NYSE: got %v, want Open", nyseBucket)
	}

	if nasdaqBucket != Open {
		t.Errorf("NASDAQ: got %v, want Open", nasdaqBucket)
	}

	if nyseBucket != nasdaqBucket {
		t.Errorf("NYSE and NASDAQ should have same bucket for same timezone")
	}
}

// TestResolveSessionBucket_UnknownExchange verifies error handling without default
func TestResolveSessionBucket_UnknownExchange(t *testing.T) {
	resolver := &SessionResolver{
		exchanges:       map[string]*ExchangeHours{},
		defaultExchange: nil,
	}

	utcTime := time.Now()
	_, err := resolver.ResolveSessionBucket(utcTime, "UNKNOWN")

	if err == nil {
		t.Error("expected error for unknown exchange without default, got nil")
	}
}

// TestResolveSessionBucket_DefaultExchange verifies fallback to default exchange
func TestResolveSessionBucket_DefaultExchange(t *testing.T) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	defaultHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	// Create resolver with default but no specific exchanges
	resolver := &SessionResolver{
		exchanges:       map[string]*ExchangeHours{},
		defaultExchange: defaultHours,
	}

	// Test with an unknown exchange identifier
	utcTime, _ := time.Parse(time.RFC3339, "2026-08-04T14:00:00Z") // 10am ET, Monday
	bucket, err := resolver.ResolveSessionBucket(utcTime, "FR")

	if err != nil {
		t.Fatalf("unexpected error for unknown exchange with default: %v", err)
	}

	if bucket != Open {
		t.Errorf("got %v, want Open", bucket)
	}

	// Test another unknown exchange
	bucket2, err := resolver.ResolveSessionBucket(utcTime, "UNKNOWN_EXCHANGE")
	if err != nil {
		t.Fatalf("unexpected error for second unknown exchange with default: %v", err)
	}

	if bucket2 != Open {
		t.Errorf("got %v, want Open", bucket2)
	}
}

// TestResolveSessionBucket_OverrideDefault verifies specific exchange overrides default
func TestResolveSessionBucket_OverrideDefault(t *testing.T) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	defaultHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	// Create a different schedule for a specific exchange
	customHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 3 * time.Hour, // Different from default
		MarketOpen:     8 * time.Hour, // Different from default
		MiddayStart:    11 * time.Hour,
		CloseStart:     14 * time.Hour,
		MarketClose:    15 * time.Hour,
		AfterHoursEnd:  19 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	resolver := &SessionResolver{
		exchanges: map[string]*ExchangeHours{
			"CUSTOM": customHours,
		},
		defaultExchange: defaultHours,
	}

	// 08:30 ET on Monday
	utcTime, _ := time.Parse(time.RFC3339, "2026-08-04T12:30:00Z")

	// CUSTOM exchange should be Open (market opens at 08:00)
	customBucket, err := resolver.ResolveSessionBucket(utcTime, "CUSTOM")
	if err != nil {
		t.Fatalf("unexpected error for CUSTOM exchange: %v", err)
	}
	if customBucket != Open {
		t.Errorf("CUSTOM: got %v, want Open", customBucket)
	}

	// Unknown exchange should use default (PreMarket at 08:30, market opens at 09:30)
	defaultBucket, err := resolver.ResolveSessionBucket(utcTime, "NYSE")
	if err != nil {
		t.Fatalf("unexpected error for NYSE using default: %v", err)
	}
	if defaultBucket != PreMarket {
		t.Errorf("NYSE (default): got %v, want PreMarket", defaultBucket)
	}
}

// TestResolveSessionBucket_OvernightSession verifies overnight detection
func TestResolveSessionBucket_OvernightSession(t *testing.T) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	nyseHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	exchanges := map[string]*ExchangeHours{"NYSE": nyseHours}
	resolver := &SessionResolver{exchanges: exchanges}

	tests := []struct {
		name     string
		time     string
		expected SessionBucket
	}{
		// After afterhours ends (20:00) until premarket (04:00)
		{"20:30 is Overnight", "2026-08-05T00:30:00Z", Overnight},
		{"22:00 is Overnight", "2026-08-05T02:00:00Z", Overnight},
		{"01:00 is Overnight", "2026-08-05T05:00:00Z", Overnight},
		{"03:30 is Overnight", "2026-08-05T07:30:00Z", Overnight},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := time.Parse(time.RFC3339, tt.time)
			if err != nil {
				t.Fatalf("failed to parse time: %v", err)
			}

			bucket, err := resolver.ResolveSessionBucket(ts, "NYSE")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if bucket != tt.expected {
				t.Errorf("got %v, want %v", bucket, tt.expected)
			}
		})
	}
}

// TestSessionAwareStats - Key acceptance test from the spec
// Verifies that overnight gaps produce low z-scores against overnight-session stats
func TestSessionAwareStats(t *testing.T) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	nyseHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	exchanges := map[string]*ExchangeHours{"NYSE": nyseHours}
	resolver := &SessionResolver{exchanges: exchanges}

	// Scenario: Create a tick sequence with an overnight gap

	// Friday 15:55 (Close session) - last tick before close
	fridayClose, _ := time.Parse(time.RFC3339, "2026-08-07T19:55:00Z") // 15:55 ET
	bucketFridayClose, _ := resolver.ResolveSessionBucket(fridayClose, "NYSE")

	// Monday 04:15 (PreMarket session) - first tick after overnight gap
	mondayPremarket, _ := time.Parse(time.RFC3339, "2026-08-10T08:15:00Z") // 04:15 ET
	bucketMondayPremarket, _ := resolver.ResolveSessionBucket(mondayPremarket, "NYSE")

	// Monday 09:35 (Open session) - tick during market hours
	mondayOpen, _ := time.Parse(time.RFC3339, "2026-08-10T13:35:00Z") // 09:35 ET
	bucketMondayOpen, _ := resolver.ResolveSessionBucket(mondayOpen, "NYSE")

	// Verify session buckets are correct
	if bucketFridayClose != Close {
		t.Errorf("Friday 15:55: got %v, want Close", bucketFridayClose)
	}

	if bucketMondayPremarket != PreMarket {
		t.Errorf("Monday 04:15: got %v, want PreMarket", bucketMondayPremarket)
	}

	if bucketMondayOpen != Open {
		t.Errorf("Monday 09:35: got %v, want Open", bucketMondayOpen)
	}

	// Calculate intertick intervals
	overnightGap := mondayPremarket.Sub(fridayClose)
	normalGap := mondayOpen.Sub(mondayPremarket)

	// Overnight gap should be ~12+ hours (43200+ seconds)
	if overnightGap < 12*time.Hour {
		t.Errorf("Expected overnight gap >= 12 hours, got %v", overnightGap)
	}

	// Normal gap should be much smaller (~5 hours for premarket to open)
	if normalGap >= overnightGap {
		t.Errorf("Normal gap should be smaller than overnight gap")
	}

	// Key test: If you maintained session-specific stats:
	// - The overnight gap is NORMAL for the PreMarket session (many hours since last Close)
	// - But would be ABNORMAL for the Open session (where typical intertick is seconds)
	//
	// This test verifies the session buckets are being resolved correctly.
	// The actual statistical calculation (z-scores) will happen in AGG-1b when
	// you maintain separate RollingStats per session bucket.

	t.Logf("Session resolution test passed:")
	t.Logf("  Friday Close tick: %v → %v", fridayClose.In(nyLoc), bucketFridayClose)
	t.Logf("  Monday PreMarket tick: %v → %v", mondayPremarket.In(nyLoc), bucketMondayPremarket)
	t.Logf("  Monday Open tick: %v → %v", mondayOpen.In(nyLoc), bucketMondayOpen)
	t.Logf("  Overnight gap: %v", overnightGap)
	t.Logf("  Normal gap: %v", normalGap)
}

// TestResolveSessionBucket_DST verifies DST transition handling
// Note: March 9, 2025 is when DST starts (spring forward)
func TestResolveSessionBucket_DST(t *testing.T) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	nyseHours := &ExchangeHours{
		Timezone:       nyLoc,
		PreMarketStart: 4 * time.Hour,
		MarketOpen:     9*time.Hour + 30*time.Minute,
		MiddayStart:    12 * time.Hour,
		CloseStart:     15*time.Hour + 30*time.Minute,
		MarketClose:    16 * time.Hour,
		AfterHoursEnd:  20 * time.Hour,
		TradingWeekdays: map[time.Weekday]bool{
			time.Monday: true, time.Tuesday: true, time.Wednesday: true,
			time.Thursday: true, time.Friday: true,
		},
	}

	exchanges := map[string]*ExchangeHours{"NYSE": nyseHours}
	resolver := &SessionResolver{exchanges: exchanges}

	// Before DST transition (EST = UTC-5)
	beforeDST, _ := time.Parse(time.RFC3339, "2025-03-07T14:30:00Z") // 09:30 EST
	bucketBefore, _ := resolver.ResolveSessionBucket(beforeDST, "NYSE")

	// After DST transition (EDT = UTC-4) - clocks spring forward
	afterDST, _ := time.Parse(time.RFC3339, "2025-03-10T13:30:00Z") // 09:30 EDT
	bucketAfter, _ := resolver.ResolveSessionBucket(afterDST, "NYSE")

	// Both should resolve to Open (09:30 local time)
	if bucketBefore != Open {
		t.Errorf("Before DST: got %v, want Open", bucketBefore)
	}

	if bucketAfter != Open {
		t.Errorf("After DST: got %v, want Open", bucketAfter)
	}

	// Verify the UTC times are different (1 hour offset change)
	// Before: 09:30 EST = 14:30 UTC
	// After:  09:30 EDT = 13:30 UTC
	t.Logf("DST handling verified:")
	t.Logf("  Before DST: %v (UTC) → %v (local) → %v", beforeDST, beforeDST.In(nyLoc), bucketBefore)
	t.Logf("  After DST:  %v (UTC) → %v (local) → %v", afterDST, afterDST.In(nyLoc), bucketAfter)
}
