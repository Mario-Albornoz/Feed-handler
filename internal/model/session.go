// Package model contains the enums regarding the time-bucket sessions which we will maintain the rolling statisctics for.
package model

import (
	"fmt"
	"time"
)

type SessionBucket int

type ExchangeHours struct {
	Timezone        *time.Location
	PreMarketStart  time.Duration
	MarketOpen      time.Duration
	MiddayStart     time.Duration
	CloseStart      time.Duration
	MarketClose     time.Duration
	AfterHoursEnd   time.Duration
	TradingWeekdays map[time.Weekday]bool
}

var sessionBucketStrings = [...]string{
	"PreMarket",
	"Open",
	"Midday",
	"Close",
	"AfterHours",
	"Overnight",
	"Weekend",
}

const (
	PreMarket SessionBucket = iota + 1
	Open
	Midday
	Close
	AfterHours
	Overnight
	Weekend
)

func (s SessionBucket) String() string {
	return sessionBucketStrings[s-1]
}

func (s SessionBucket) EnumIndex() int {
	return int(s)
}

type SessionResolver struct {
	exchanges       map[string]*ExchangeHours
	defaultExchange *ExchangeHours
}

func NewSessionResolver(exchanges map[string]*ExchangeHours) *SessionResolver {
	return &SessionResolver{
		exchanges:       exchanges,
		defaultExchange: nil,
	}
}

func NewSessionResolverWithDefault(defaultExchange *ExchangeHours, exchanges map[string]*ExchangeHours) *SessionResolver {
	return &SessionResolver{
		exchanges:       exchanges,
		defaultExchange: defaultExchange,
	}
}

func (sr *SessionResolver) ResolveSessionBucket(t time.Time, exchange string) (SessionBucket, error) {
	// Try to find exchange-specific hours first
	exch, ok := sr.exchanges[exchange]
	if !ok {
		// Fall back to default exchange if available
		if sr.defaultExchange == nil {
			return 0, fmt.Errorf("unknown exchange: %s and no default exchange configured", exchange)
		}
		exch = sr.defaultExchange
	}

	localTime := t.In(exch.Timezone)
	weekday := localTime.Weekday()

	if !exch.TradingWeekdays[weekday] {
		return Weekend, nil
	}

	timeOfDay := time.Duration(localTime.Hour())*time.Hour +
		time.Duration(localTime.Minute())*time.Minute +
		time.Duration(localTime.Second())*time.Second

	if timeOfDay >= exch.PreMarketStart && timeOfDay < exch.MarketOpen {
		return PreMarket, nil
	}
	if timeOfDay >= exch.MarketOpen && timeOfDay < exch.MiddayStart {
		return Open, nil
	}
	if timeOfDay >= exch.MiddayStart && timeOfDay < exch.CloseStart {
		return Midday, nil
	}
	if timeOfDay >= exch.CloseStart && timeOfDay < exch.MarketClose {
		return Close, nil
	}
	if timeOfDay >= exch.MarketClose && timeOfDay < exch.AfterHoursEnd {
		return AfterHours, nil
	}

	return Overnight, nil
}
