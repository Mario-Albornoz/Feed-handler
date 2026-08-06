package model

import (
	"encoding/gob"
	"os"
	"sync"
)

type InstrumentRegistry struct {
	mu          sync.RWMutex
	instruments map[InstrumentKey]*InstrumentState

	fastWindow float64
	slowWindow float64
	cusumSlack float64
}

func NewInstrumentRegistry(fastWindow, slowWindow, cusumSlack float64) *InstrumentRegistry {
	return &InstrumentRegistry{
		instruments: make(map[InstrumentKey]*InstrumentState),
		fastWindow:  fastWindow,
		slowWindow:  slowWindow,
		cusumSlack:  cusumSlack,
	}
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
	r.instruments[key] = state

	return state

}

func (r *InstrumentRegistry) All() map[InstrumentKey]*InstrumentState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.instruments
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
