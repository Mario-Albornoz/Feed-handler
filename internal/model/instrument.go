// Package model contains the enums regarding the time-bucket sessions which we will maintain the rolling statisctics for.
package model

import "time"

type SessionBucket int

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

func ResolveSessionBucket(t time.Time, exchange string) SessionBucket {

}
