package model

import (
	"time"
)

type RawTick struct {
	ID       string `json:"ID"` // Symbol with exchange: "RDSA.NL"
	Exchange string `json:"Exchange"`
	SecType  string `json:"SecType"` // "E" (equity) or "I" (index)
	ISIN     string `json:"ISIN"`    // International Securities ID

	Bid float64 `json:"Bid"`
	Ask float64 `json:"Ask"`
	// LastTradedPrice is "Last" on the wire: the simulator marshals its tick struct, whose
	// field carries the CSV column name. A different name here decodes silently to 0, so
	// no price ever reaches the pipeline (see TestWireFormat_PriceReachesTheHandler).
	LastTradedPrice float64 `json:"Last"`
	TotalVolume     float64 `json:"TotalVolume"`

	// Seq is the simulator's run-wide message number; it is passed on to the vector so the
	// evaluation can identify the exact message (see NormalizedVector.Seq).
	Seq uint64 `json:"Seq"`

	TradingTime time.Time `json:"TradingTime"` // Millisecond timestamp when the row has one, else the update time
	Date        time.Time `json:"Date"`        // System date (optional)
	Time        time.Time `json:"Time"`        // System update time, whole seconds (optional)
}

// FeatureTime is the clock the timing features and the silence detector run on: the
// whole-second update time. TradingTime mixes millisecond and whole-second values, so
// an instrument's TradingTime steps backwards about 30% of the time by up to a second;
// the update time is monotone. TradingTime stays the identifier of a message (vector
// timestamp, ground-truth matching, the validator). Falls back to TradingTime when a
// message has no update time.
func (t *RawTick) FeatureTime() time.Time {
	if !t.Time.IsZero() {
		return t.Time
	}
	return t.TradingTime
}
