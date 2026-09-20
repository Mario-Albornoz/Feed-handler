package silence

import (
	"context"
	"errors"
	"strconv"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/alertlog"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

// LogHeader is the header of the silence alert evaluation log. Times are event time
// in epoch milliseconds, except WallTimeMs.
var LogHeader = []string{
	"Exchange", "Instrument", "AlertType",
	"LastSeenMs", "DetectedAtMs", "ObservedAtMs",
	"ElapsedMs", "ThresholdMs", "ExpectedIntervalMs",
	"Level", "Trigger", "WallTimeMs",
}

// LogEmitter writes silence alerts to the evaluation log.
type LogEmitter struct {
	log *alertlog.CSVLog
}

func NewLogEmitter(log *alertlog.CSVLog) *LogEmitter {
	return &LogEmitter{log: log}
}

func (e *LogEmitter) WriteAlert(_ context.Context, alert *model.SilenceAlert) error {
	return e.log.Write([]string{
		alert.Exchange,
		alert.Instrument,
		alert.AlertType,
		alertlog.Ms(alert.LastSeen),
		alertlog.Ms(alert.DetectedAt),
		alertlog.Ms(alert.ObservedAt),
		strconv.FormatInt(alert.ElapsedMs, 10),
		strconv.FormatFloat(alert.ThresholdMs, 'f', 1, 64),
		strconv.FormatFloat(alert.ExpectedInterval, 'f', 1, 64),
		alert.LatencyLevel,
		alert.Trigger,
		strconv.FormatInt(alert.Timestamp.UnixMilli(), 10),
	})
}

// MultiEmitter fans an alert out to several emitters. A failing emitter does not
// stop the others; the errors are joined.
type MultiEmitter []AlertEmitter

func (m MultiEmitter) WriteAlert(ctx context.Context, alert *model.SilenceAlert) error {
	var errs []error
	for _, emitter := range m {
		if err := emitter.WriteAlert(ctx, alert); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
