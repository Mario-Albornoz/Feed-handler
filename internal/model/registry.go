package model

import (
	"encoding/gob"
	"os"
	"sync"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
)

type InstrumentRegistry struct {
	mu          sync.RWMutex
	instruments map[InstrumentKey]*InstrumentState

	fastWindow float64
	slowWindow float64
	cusumSlack float64

	minPriceObservations int64
	limits               *stats.Limits
}

func NewInstrumentRegistry(fastWindow, slowWindow, cusumSlack float64) *InstrumentRegistry {
	return &InstrumentRegistry{
		instruments: make(map[InstrumentKey]*InstrumentState),
		fastWindow:  fastWindow,
		slowWindow:  slowWindow,
		cusumSlack:  cusumSlack,
	}
}

// SetLimits sets the z-score limits for instruments created from now on (call it
// before ticks flow). Without it instruments use stats.DefaultLimits.
func (r *InstrumentRegistry) SetLimits(limits stats.Limits) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.limits = &limits
}

// SetMinPriceObservations sets the price warm-up requirement for instruments created
// from now on (call it before ticks flow).
func (r *InstrumentRegistry) SetMinPriceObservations(n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.minPriceObservations = n
}

func (r *InstrumentRegistry) GetOrCreate(key InstrumentKey) *InstrumentState {
	r.mu.RLock()
	state, exists := r.instruments[key]
	r.mu.RUnlock()
	if exists {
		return state
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	state, exists = r.instruments[key]
	if exists {
		return state
	}
	state = NewInstrumentState(r.fastWindow, r.slowWindow, r.cusumSlack)
	if r.minPriceObservations > 0 {
		state.SetMinPriceObservations(r.minPriceObservations)
	}
	if r.limits != nil {
		state.SetLimits(*r.limits)
	}
	r.instruments[key] = state

	return state

}

// All returns a snapshot of the registry. The map is a copy, so callers can range
// over it while the consumer goroutine registers new instruments; the states are
// shared (see InstrumentState for their locking).
func (r *InstrumentRegistry) All() map[InstrumentKey]*InstrumentState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshot := make(map[InstrumentKey]*InstrumentState, len(r.instruments))
	for key, state := range r.instruments {
		snapshot[key] = state
	}
	return snapshot
}

func (r *InstrumentRegistry) Save(path string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	return encoder.Encode(r.instruments)
}

func (r *InstrumentRegistry) Load(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	defer file.Close()
	r.mu.Lock()
	defer r.mu.Unlock()

	decoder := gob.NewDecoder(file)
	return decoder.Decode(&r.instruments)
}
