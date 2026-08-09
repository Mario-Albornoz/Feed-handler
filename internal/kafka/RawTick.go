package kafka

import "time"

type RawTick struct {
	Id            string
	SecType       string
	Date          time.Time
	Time          time.Time
	Ask           float64
	AskVolume     float64
	Bid           float64
	BidVolume     float64
	AskTime       time.Time
	Close         float64
	Isin          string
	Open          float64
	TradingTime   time.Time
	TotalVolume   float64
	CurrentPrices float64
}
