package model

import (
	"time"
)

type RawTick struct {
	ID       string `json:"ID"` // Symbol with exchange: "RDSA.NL"
	Exchange string `json:"Exchange"`
	SecType  string `json:"SecType"` // "E" (equity) or "I" (index)
	ISIN     string `json:"ISIN"`    // International Securities ID

	Bid             float64 `json:"Bid"`
	Ask             float64 `json:"Ask"`
	LastTradedPrice float64 `json:"Close"`
	TotalVolume     float64 `json:"TotalVolume"`

	TradingTime time.Time `json:"TradingTime"` // Timestamp of last update
	Date        time.Time `json:"Date"`        // System date (optional)
	Time        time.Time `json:"Time"`        // System time (optional)
}
