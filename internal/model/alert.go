package model

import "time"

// Silence alert triggers: what revealed the silence.
const (
	// TriggerScan: the periodic scan saw an instrument that is still quiet.
	TriggerScan = "scan"
	// TriggerResume: the instrument ticked again after a gap the scan had not
	// reported. Under a fast replay this is the usual path, because the scan runs on
	// the wall clock.
	TriggerResume = "resume"
	// TriggerFlush: final scan at shutdown, against the last watermark.
	TriggerFlush = "flush"
)

// SilenceAlert reports that an instrument has gone quiet for longer than its own
// learned tick interval allows. All times except Timestamp are event time.
type SilenceAlert struct {
	Exchange         string    `json:"exchange"`
	Instrument       string    `json:"instrument"`
	AlertType        string    `json:"alert_type"`           // always "SILENCE"
	LastSeen         time.Time `json:"last_seen"`            // last tick before the silence
	ElapsedMs        int64     `json:"elapsed_ms"`           // silence length when the alert was raised
	ExpectedInterval float64   `json:"expected_interval_ms"` // the instrument's mean gap, for context
	LatencyLevel     string    `json:"latency_level"`        // "LOW", "MEDIUM", "SEVERE"
	Timestamp        time.Time `json:"timestamp"`            // wall-clock emission time

	// DetectedAt is the event time at which the silence first exceeded the
	// threshold (LastSeen + ThresholdMs). It does not depend on when the scan ran,
	// so detection latency is reproducible at any replay speed.
	DetectedAt  time.Time `json:"detected_at"`
	ThresholdMs float64   `json:"threshold_ms"`
	// ObservedAt is the event time of the clock reading or tick that revealed the
	// silence, and Trigger says which one (see the Trigger* constants).
	ObservedAt time.Time `json:"observed_at"`
	Trigger    string    `json:"trigger"`
}

// DetermineLatencyLevel grades a silence by how far it exceeds the silence threshold
// (the instrument's own high gap quantile).
func DetermineLatencyLevel(elapsedMs float64, thresholdMs float64) string {
	if thresholdMs <= 0 {
		return "UNKNOWN"
	}

	ratio := elapsedMs / thresholdMs

	// Severe: more than 10x the threshold (catastrophic failure)
	if ratio > 10.0 {
		return "SEVERE"
	}
	// Medium: more than 3x the threshold (serious degradation)
	if ratio > 3.0 {
		return "MEDIUM"
	}
	// Low: past the threshold (noticeable but not critical)
	return "LOW"
}

// Validation alert types.
const (
	AlertMalformedISIN      = "MALFORMED_ISIN"
	AlertTimestampInversion = "TIMESTAMP_INVERSION"
)

// ValidationAlert reports a tick rejected by the feed-integrity validator. Rejected
// ticks never reach the feature pipeline. All times except Timestamp are event time.
type ValidationAlert struct {
	Exchange   string `json:"exchange"`
	Instrument string `json:"instrument"`
	AlertType  string `json:"alert_type"`

	// TickTime is the event time carried by the rejected tick (after any corruption).
	TickTime time.Time `json:"tick_time"`
	// ReferenceTime is the last accepted tick of the instrument, for rules that
	// compare against it (timestamp inversion). Zero otherwise.
	ReferenceTime time.Time `json:"reference_time,omitempty"`
	Detail        string    `json:"detail"`
	Timestamp     time.Time `json:"timestamp"` // wall-clock emission time
}
