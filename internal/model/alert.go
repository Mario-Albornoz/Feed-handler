package model

type SilenceAlert struct {
	InstrumentIdentifier string
	Exchange             string
	LatencyLevel         LatencyLevels
}

func NewAlert(instrumentIdentifier string, exchange string, latencyLevel LatencyLevels) *SilenceAlert {
	return &SilenceAlert{
		InstrumentIdentifier: instrumentIdentifier,
		Exchange:             exchange,
		LatencyLevel:         latencyLevel,
	}
}

type LatencyLevels int

const (
	Severe LatencyLevels = iota + 1
	Medium
	Low
)

func (l LatencyLevels) String() string {
	switch l {
	case Severe:
		return "Severe"
	case Medium:
		return "Medium"
	case Low:
		return "Low"
	default:
		return "Unkown"
	}
}
