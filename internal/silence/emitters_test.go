package silence

import (
	"context"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/alertlog"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

func TestLogEmitterWritesEvaluationRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "eval", "silence.csv")
	l, err := alertlog.Open(path, LogHeader)
	if err != nil {
		t.Fatal(err)
	}

	alert := &model.SilenceAlert{
		Exchange:         "ETR",
		Instrument:       "SAP.ETR",
		AlertType:        "SILENCE",
		LastSeen:         t0,
		DetectedAt:       t0.Add(500 * time.Millisecond),
		ObservedAt:       t0.Add(10 * time.Second),
		ElapsedMs:        10000,
		ThresholdMs:      500,
		ExpectedInterval: 100,
		LatencyLevel:     "SEVERE",
		Trigger:          model.TriggerResume,
		Timestamp:        time.UnixMilli(1700000000000),
	}
	if err := NewLogEmitter(l).WriteAlert(context.Background(), alert); err != nil {
		t.Fatal(err)
	}
	l.Close()

	f, _ := os.Open(path)
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil || len(rows) != 2 {
		t.Fatalf("expected header + 1 row, got %v (err %v)", rows, err)
	}

	got := map[string]string{}
	for i, h := range rows[0] {
		got[h] = rows[1][i]
	}
	want := map[string]string{
		"Exchange":           "ETR",
		"Instrument":         "SAP.ETR",
		"AlertType":          "SILENCE",
		"LastSeenMs":         strconv.FormatInt(t0.UnixMilli(), 10),
		"DetectedAtMs":       strconv.FormatInt(t0.UnixMilli()+500, 10),
		"ObservedAtMs":       strconv.FormatInt(t0.UnixMilli()+10000, 10),
		"ElapsedMs":          "10000",
		"ThresholdMs":        "500.0",
		"ExpectedIntervalMs": "100.0",
		"Level":              "SEVERE",
		"Trigger":            "resume",
		"WallTimeMs":         "1700000000000",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %q, want %q", k, got[k], v)
		}
	}
}

type failingEmitter struct{}

func (failingEmitter) WriteAlert(context.Context, *model.SilenceAlert) error {
	return errors.New("boom")
}

func TestMultiEmitterContinuesAfterFailure(t *testing.T) {
	ok := &MockAlertEmitter{}
	multi := MultiEmitter{failingEmitter{}, ok}

	err := multi.WriteAlert(context.Background(), &model.SilenceAlert{})
	if err == nil {
		t.Error("expected the failing emitter's error to be reported")
	}
	if ok.count() != 1 {
		t.Errorf("the healthy emitter should still receive the alert, got %d", ok.count())
	}
}
