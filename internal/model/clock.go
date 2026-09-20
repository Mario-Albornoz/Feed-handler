package model

import (
	"context"
	"sync"
	"time"
)

type partitionKey struct{}

// WithPartition records on ctx which Kafka partition a message came from.
func WithPartition(ctx context.Context, partition int) context.Context {
	return context.WithValue(ctx, partitionKey{}, partition)
}

// PartitionFrom returns the partition recorded by WithPartition (0, false if none).
func PartitionFrom(ctx context.Context) (int, bool) {
	p, ok := ctx.Value(partitionKey{}).(int)
	return p, ok
}

// EventClock tracks event time per exchange as a watermark: how far the feed's own time
// has progressed.
//
// The silence detector needs "now" to decide whether an instrument has gone quiet.
// Wall-clock time is meaningless when a recorded day is replayed at an arbitrary speed,
// so silence is measured against the feed's own time instead.
//
// Messages arrive on several Kafka partitions that are consumed at different speeds
// (each keeps its own order, but they can be minutes of event time apart when the
// consumer lags), so the latest time seen on the exchange could run far ahead of an
// instrument that is merely waiting in a slower partition and make it look silent. The
// exchange clock is therefore the minimum over the partitions that have delivered
// something: it never gets ahead of any partition's progress.
//
// Limitations: a partition that goes quiet holds the clock back (silence alerts are
// delayed, not invented); and if an entire exchange goes quiet its clock stalls, so an
// exchange-wide outage is not visible to it (a live deployment would fall back to the
// wall clock for that case).
type EventClock struct {
	mu    sync.RWMutex
	marks map[string]map[int]time.Time // exchange -> partition -> latest event time
}

func NewEventClock() *EventClock {
	return &EventClock{marks: make(map[string]map[int]time.Time)}
}

// Advance moves the exchange's watermark on partition 0 forward to t (for callers that
// have no partition). Earlier times are ignored.
func (c *EventClock) Advance(exchange string, t time.Time) {
	c.AdvancePartition(exchange, 0, t)
}

// AdvancePartition moves the watermark of one partition of an exchange forward to t.
// Earlier times are ignored.
func (c *EventClock) AdvancePartition(exchange string, partition int, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	parts := c.marks[exchange]
	if parts == nil {
		parts = make(map[int]time.Time)
		c.marks[exchange] = parts
	}
	if current, ok := parts[partition]; !ok || t.After(current) {
		parts[partition] = t
	}
}

// Now returns the exchange's clock: the slowest partition's watermark. The second
// result is false if no message has been seen on that exchange yet.
func (c *EventClock) Now(exchange string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	parts := c.marks[exchange]
	if len(parts) == 0 {
		return time.Time{}, false
	}
	var min time.Time
	first := true
	for _, t := range parts {
		if first || t.Before(min) {
			min, first = t, false
		}
	}
	return min, true
}
