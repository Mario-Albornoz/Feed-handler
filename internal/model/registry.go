package model

import (
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
