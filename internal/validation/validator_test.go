package validation

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/alertlog"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

type captureEmitter struct {
	mu     sync.Mutex
	alerts []*model.ValidationAlert
}

func (c *captureEmitter) WriteValidationAlert(_ context.Context, a *model.ValidationAlert) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alerts = append(c.alerts, a)
	return nil
}

var base = time.Date(2021, 11, 11, 10, 0, 0, 0, time.UTC)

func tick(isin string, at time.Time) *model.RawTick {
	return &model.RawTick{ID: "SAP.ETR", Exchange: "ETR", SecType: "E", ISIN: isin, TradingTime: at}
}

func TestCheckISIN(t *testing.T) {
	v := New(1000, nil)

	cases := []struct {
		name    string
		isin    string
		flagged bool
	}{
		{"valid", "DE0007164600", false},
		{"valid with letters", "US0378331005", false},
		{"absent (DEBS data has no ISIN)", "", false},
		{"whitespace only", "   ", false},
		{"truncated", "DE0007", true},
		{"junk appended", "DE0007164600XXX", true},
		{"junk on an absent ISIN", "XXX", true},
		{"lowercase", "de0007164600", true},
		{"check digit not numeric", "DE000716460A", true},
	}
	for _, c := range cases {
		alerts := v.Check(tick(c.isin, base), time.Time{})
		if (len(alerts) == 1) != c.flagged || len(alerts) > 1 {
			t.Errorf("%s: flagged=%v, alerts=%v", c.name, len(alerts) == 1, alerts)
			continue
		}
		if c.flagged && alerts[0].AlertType != model.AlertMalformedISIN {
			t.Errorf("%s: wrong alert type %q", c.name, alerts[0].AlertType)
		}
	}
}

func TestCheckTimestampInversion(t *testing.T) {
	v := New(1000, nil)
	last := base

	cases := []struct {
		name    string
		at      time.Time
		last    time.Time
		flagged bool
	}{
		{"forward", base.Add(time.Second), last, false},
		{"same time", base, last, false},
		{"normal backward noise (500ms)", base.Add(-500 * time.Millisecond), last, false},
		{"exactly at tolerance", base.Add(-time.Second), last, false},
		{"just past tolerance", base.Add(-1001 * time.Millisecond), last, true},
		{"rewound 5 minutes", base.Add(-5 * time.Minute), last, true},
		{"first tick of an instrument", base.Add(-time.Hour), time.Time{}, false},
	}
	for _, c := range cases {
		alerts := v.Check(tick("", c.at), c.last)
		if (len(alerts) == 1) != c.flagged {
			t.Errorf("%s: flagged=%v, alerts=%v", c.name, len(alerts) == 1, alerts)
			continue
		}
		if c.flagged {
			a := alerts[0]
			if a.AlertType != model.AlertTimestampInversion || !a.ReferenceTime.Equal(c.last) || !a.TickTime.Equal(c.at) {
				t.Errorf("%s: unexpected alert %+v", c.name, a)
			}
		}
	}
}

func TestValidateRejectsAndReports(t *testing.T) {
	emitter := &captureEmitter{}
	v := New(1000, emitter)
	ctx := context.Background()

	if !v.Validate(ctx, tick("DE0007164600", base), base.Add(-time.Second)) {
		t.Fatal("clean tick must be accepted")
	}
	if len(emitter.alerts) != 0 {
		t.Fatalf("clean tick must not alert, got %d", len(emitter.alerts))
	}

	// One tick can break both rules; it is rejected once and reports both.
	if v.Validate(ctx, tick("XXX", base.Add(-time.Minute)), base) {
		t.Fatal("malformed tick must be rejected")
	}
	if len(emitter.alerts) != 2 {
		t.Fatalf("expected 2 alerts, got %d", len(emitter.alerts))
	}
	for _, a := range emitter.alerts {
		if a.Timestamp.IsZero() {
			t.Error("wall-clock timestamp should be set")
		}
	}

	rejected, isins, inversions := v.Counts()
	if rejected != 1 || isins != 1 || inversions != 1 {
		t.Errorf("counts: rejected=%d isins=%d inversions=%d, want 1/1/1", rejected, isins, inversions)
	}
}

func TestLogEmitterWritesEvaluationRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "validation.csv")
	l, err := alertlog.Open(path, LogHeader)
	if err != nil {
		t.Fatal(err)
	}
	v := New(1000, NewLogEmitter(l))

	rewound := base.Add(-time.Minute)
	v.Validate(context.Background(), tick("", rewound), base)
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
	if got["AlertType"] != model.AlertTimestampInversion ||
		got["TickTimeMs"] != strconv.FormatInt(rewound.UnixMilli(), 10) ||
		got["ReferenceTimeMs"] != strconv.FormatInt(base.UnixMilli(), 10) ||
		got["Instrument"] != "SAP.ETR" || got["Exchange"] != "ETR" {
		t.Errorf("unexpected row: %v", got)
	}
}
