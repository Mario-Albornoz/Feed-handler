package model

import "time"

type SilenceAlert struct {
	Exchange         string    `json:"exchange"`
	Instrument       string    `json:"instrument"`
	AlertType        string    `json:"alert_type"` // always "SILENCE"
	LastSeen         time.Time `json:"last_seen"`
	ElapsedMs        int64     `json:"elapsed_ms"`
	ExpectedInterval float64   `json:"expected_interval_ms"`
	LatencyLevel     string    `json:"latency_level"` // "LOW", "MEDIUM", "SEVERE"
	Timestamp        time.Time `json:"timestamp"`
}

func DetermineLatencyLevel(elapsedMs float64, expectedIntervalMs float64) string {
	if expectedIntervalMs <= 0 {
		return "UNKNOWN"
	}

	ratio := elapsedMs / expectedIntervalMs

	// Severe: > 20x expected interval (catastrophic failure)
	if ratio > 20.0 {
		return "SEVERE"
	}
	// Medium: > 10x expected interval (serious degradation)
	if ratio > 10.0 {
		return "MEDIUM"
	}
	// Low: > gap_multiplier (default 5x) but < 10x (noticeable but not critical)
	return "LOW"
}
