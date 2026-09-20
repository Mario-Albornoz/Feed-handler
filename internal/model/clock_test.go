package model

import (
	"context"
	"testing"
	"time"
)

func TestEventClockIsMonotonicPerExchange(t *testing.T) {
	c := NewEventClock()
	base := time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)

	if _, ok := c.Now("ETR"); ok {
		t.Fatal("no reading expected before the first tick")
	}

	c.Advance("ETR", base)
	c.Advance("ETR", base.Add(-time.Minute)) // a rewound tick must not move it back
	if now, _ := c.Now("ETR"); !now.Equal(base) {
		t.Errorf("clock moved backwards: %v", now)
	}

	c.Advance("ETR", base.Add(time.Second))
	if now, _ := c.Now("ETR"); !now.Equal(base.Add(time.Second)) {
		t.Errorf("clock did not advance: %v", now)
	}

	if _, ok := c.Now("FR"); ok {
		t.Error("exchanges must be independent")
	}
}

func TestRegistryAllReturnsSnapshot(t *testing.T) {
	r := NewInstrumentRegistry(50, 200, 0.5)
	r.GetOrCreate(InstrumentKey{Source: "ETR", InstrumentIdentifier: "A"})

	snapshot := r.All()
	r.GetOrCreate(InstrumentKey{Source: "ETR", InstrumentIdentifier: "B"})

	if len(snapshot) != 1 {
		t.Errorf("snapshot changed after a later registration: %d entries", len(snapshot))
	}
	if len(r.All()) != 2 {
		t.Errorf("registry should hold 2 instruments, got %d", len(r.All()))
	}
}

// Partitions are consumed at different speeds: the exchange clock must not run ahead of
// the slowest one, or an instrument waiting in a slow partition looks silent.
func TestEventClockIsTheSlowestPartition(t *testing.T) {
	c := NewEventClock()
	base := time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)

	c.AdvancePartition("ETR", 0, base.Add(10*time.Minute))
	c.AdvancePartition("ETR", 1, base)
	if now, _ := c.Now("ETR"); !now.Equal(base) {
		t.Errorf("clock should be the slower partition (%v), got %v", base, now)
	}

	c.AdvancePartition("ETR", 1, base.Add(5*time.Minute))
	if now, _ := c.Now("ETR"); !now.Equal(base.Add(5 * time.Minute)) {
		t.Errorf("clock should follow the slower partition forward, got %v", now)
	}

	c.AdvancePartition("ETR", 1, base) // backwards: ignored
	if now, _ := c.Now("ETR"); !now.Equal(base.Add(5 * time.Minute)) {
		t.Errorf("a partition's watermark never moves backwards, got %v", now)
	}
}

func TestPartitionRoundTripsThroughContext(t *testing.T) {
	ctx := WithPartition(context.Background(), 2)
	if p, ok := PartitionFrom(ctx); !ok || p != 2 {
		t.Errorf("got %d, %v", p, ok)
	}
	if _, ok := PartitionFrom(context.Background()); ok {
		t.Error("a context without a partition must say so")
	}
}
