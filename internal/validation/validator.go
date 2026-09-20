// Package validation implements the feed-integrity checks for malformed messages.
//
// These are rule-based checks, not statistical ones: a message whose ISIN is
// garbled or whose timestamp jumps backwards is a fault of the feed itself, and no
// learned baseline is needed to see it. Like silence detection it is kept separate
// from the RRCF feature pipeline. A rejected tick is quarantined (it never updates
// instrument state or reaches the detector) and reported as an alert.
package validation

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/alertlog"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

// isinPattern is the ISO 6166 shape: 2-letter country code, 9 alphanumeric
// characters and a numeric check digit. The check digit itself is not verified.
var isinPattern = regexp.MustCompile(`^[A-Z]{2}[A-Z0-9]{9}[0-9]$`)

type AlertEmitter interface {
	WriteValidationAlert(ctx context.Context, alert *model.ValidationAlert) error
}

// Validator applies the integrity rules to ticks.
type Validator struct {
	toleranceMs int64
	emitter     AlertEmitter

	rejected        uint64
	malformedISINs  uint64
	timestampInvert uint64
}

// New returns a validator. timestampToleranceMs is how far behind an instrument's
// last accepted tick a timestamp may fall before it counts as an inversion. Update
// times in the data are only reliable to about a second (trade times carry whole
// seconds while quote updates carry milliseconds), so an instrument's timestamps
// step backwards by up to a second in normal operation.
func New(timestampToleranceMs int64, emitter AlertEmitter) *Validator {
	return &Validator{toleranceMs: timestampToleranceMs, emitter: emitter}
}

// Check returns the rules the tick violates (none if it is clean). lastTick is the
// event time of the instrument's last accepted tick, or the zero time if it has
// none.
//
// An empty ISIN is not flagged: the field is absent from the DEBS 2022 data, and a
// missing value is not a malformed one.
func (v *Validator) Check(tick *model.RawTick, lastTick time.Time) []*model.ValidationAlert {
	var alerts []*model.ValidationAlert

	if isin := strings.TrimSpace(tick.ISIN); isin != "" && !isinPattern.MatchString(isin) {
		alerts = append(alerts, &model.ValidationAlert{
			Exchange:   tick.Exchange,
			Instrument: tick.ID,
			AlertType:  model.AlertMalformedISIN,
			TickTime:   tick.TradingTime,
			Detail:     fmt.Sprintf("isin=%s;length=%d", isin, len(isin)),
		})
	}

	if !lastTick.IsZero() {
		backwardMs := lastTick.Sub(tick.TradingTime).Milliseconds()
		if backwardMs > v.toleranceMs {
			alerts = append(alerts, &model.ValidationAlert{
				Exchange:      tick.Exchange,
				Instrument:    tick.ID,
				AlertType:     model.AlertTimestampInversion,
				TickTime:      tick.TradingTime,
				ReferenceTime: lastTick,
				Detail:        fmt.Sprintf("backward_ms=%d;tolerance_ms=%d", backwardMs, v.toleranceMs),
			})
		}
	}

	return alerts
}

// Validate checks the tick, reports every violation to the emitter and returns
// whether the tick is clean. A tick that returns false must be quarantined.
func (v *Validator) Validate(ctx context.Context, tick *model.RawTick, lastTick time.Time) bool {
	alerts := v.Check(tick, lastTick)
	if len(alerts) == 0 {
		return true
	}

	atomic.AddUint64(&v.rejected, 1)
	now := time.Now()
	for _, alert := range alerts {
		alert.Timestamp = now
		switch alert.AlertType {
		case model.AlertMalformedISIN:
			atomic.AddUint64(&v.malformedISINs, 1)
		case model.AlertTimestampInversion:
			atomic.AddUint64(&v.timestampInvert, 1)
		}
		if v.emitter != nil {
			// A failed write must not stop the feed; the tick is rejected regardless.
			_ = v.emitter.WriteValidationAlert(ctx, alert)
		}
	}
	return false
}

// Counts returns the number of rejected ticks and the alerts raised per rule.
func (v *Validator) Counts() (rejected, malformedISINs, timestampInversions uint64) {
	return atomic.LoadUint64(&v.rejected),
		atomic.LoadUint64(&v.malformedISINs),
		atomic.LoadUint64(&v.timestampInvert)
}

// LogHeader is the header of the validation alert evaluation log. Times are event
// time in epoch milliseconds, except WallTimeMs. For timestamp inversions TickTimeMs
// is the (rewound) time the tick carried, which is what the simulator's ground truth
// records as ObservedMs.
var LogHeader = []string{
	"Exchange", "Instrument", "AlertType",
	"TickTimeMs", "ReferenceTimeMs", "Detail", "WallTimeMs",
}

// LogEmitter writes validation alerts to the evaluation log.
type LogEmitter struct {
	log *alertlog.CSVLog
}

func NewLogEmitter(log *alertlog.CSVLog) *LogEmitter {
	return &LogEmitter{log: log}
}

func (e *LogEmitter) WriteValidationAlert(_ context.Context, alert *model.ValidationAlert) error {
	return e.log.Write([]string{
		alert.Exchange,
		alert.Instrument,
		alert.AlertType,
		alertlog.Ms(alert.TickTime),
		alertlog.Ms(alert.ReferenceTime),
		alert.Detail,
		strconv.FormatInt(alert.Timestamp.UnixMilli(), 10),
	})
}
