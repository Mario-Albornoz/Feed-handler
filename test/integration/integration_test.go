// Package integration runs the real aggregator binary against a real Kafka and drives it
// with messages in the simulator's exact wire format.
//
// It exists to answer, before a long run, "does the whole chain do what we think?":
// prices arrive, quote rows carry no price step, a rewound timestamp is quarantined, a
// silent instrument is reported, index rows are ignored, and the evaluation logs are
// written. Each check reports what it saw, so a failure says what went wrong. The 2021
// run that had no price features at all would have failed here.
//
// Requires Kafka on localhost:9092 (docker compose up) and INTEGRATION_TEST=1:
//
//	INTEGRATION_TEST=1 go test ./test/integration/... -v -timeout=5m
package integration

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/segmentio/kafka-go"
)

const testBroker = "localhost:9092"

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION_TEST") != "1" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// ---- the simulator's wire format --------------------------------------------------

// wireTick mirrors the simulator's model.RawTick JSON tags exactly. It is the contract
// between the two programs; do not "fix" a name here to match the handler.
type wireTick struct {
	ID       string  `json:"ID"`
	Exchange string  `json:"Exchange"`
	SecType  string  `json:"SecType"`
	ISIN     string  `json:"ISIN"`
	Bid      float64 `json:"Bid"`
	Ask      float64 `json:"Ask"`
	Volume   float64 `json:"TotalVolume"`
	Last     float64 `json:"Last"`
	Seq      uint64  `json:"Seq"`

	TradingTime time.Time `json:"TradingTime"`
	Date        time.Time `json:"Date"`
	Time        time.Time `json:"Time"`

	AnomalyInjected bool   `json:"anomaly_injected,omitempty"`
	AnomalyType     string `json:"anomaly_type,omitempty"`
}

var day = time.Date(2021, 11, 10, 0, 0, 0, 0, time.UTC)

// msg builds a message at whole second `sec` after 10:00:00. Trades carry a millisecond
// TradingTime, quote updates only the whole second, as in the data.
func msg(id, exchange string, sec int, price float64) wireTick {
	updateTime := day.Add(10*time.Hour + time.Duration(sec)*time.Second)
	trading := updateTime
	if price > 0 {
		trading = updateTime.Add(412 * time.Millisecond)
	}
	return wireTick{ID: id, Exchange: exchange, SecType: "E", Last: price,
		TradingTime: trading, Date: day, Time: updateTime}
}

// ---- infrastructure ----------------------------------------------------------------

var buildOnce sync.Once
var binaryPath string
var buildErr error

// aggregatorBinary builds the aggregator once per test run.
func aggregatorBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "aggregator-bin")
		if err != nil {
			buildErr = err
			return
		}
		binaryPath = filepath.Join(dir, "aggregator")
		cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/aggregator")
		cmd.Dir = "../.."
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build failed: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatalf("cannot build the aggregator: %v", buildErr)
	}
	return binaryPath
}

type topics struct{ input, vectors, alerts string }

func createTopics(t *testing.T, tp topics) {
	t.Helper()
	conn, err := kafka.Dial("tcp", testBroker)
	if err != nil {
		t.Fatalf("cannot reach Kafka at %s (is docker compose up?): %v", testBroker, err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}
	cc, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	err = cc.CreateTopics(
		kafka.TopicConfig{Topic: tp.input, NumPartitions: 3, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: tp.vectors, NumPartitions: 3, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: tp.alerts, NumPartitions: 1, ReplicationFactor: 1},
	)
	if err != nil {
		t.Fatalf("cannot create topics: %v", err)
	}
	waitForLeaders(t, tp)
	t.Cleanup(func() {
		if c, err := kafka.Dial("tcp", testBroker); err == nil {
			defer c.Close()
			if ctrl, err := c.Controller(); err == nil {
				if cc, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", ctrl.Host, ctrl.Port)); err == nil {
					defer cc.Close()
					cc.DeleteTopics(tp.input, tp.vectors, tp.alerts)
				}
			}
		}
	})
}

// waitForLeaders blocks until every partition of the new topics has a leader; writing
// earlier makes the writer retry, and retries can reorder a partition's messages.
func waitForLeaders(t *testing.T, tp topics) {
	t.Helper()
	conn, err := kafka.Dial("tcp", testBroker)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline := time.Now().Add(20 * time.Second)
	for _, topic := range []string{tp.input, tp.vectors, tp.alerts} {
		for {
			parts, err := conn.ReadPartitions(topic)
			ready := err == nil && len(parts) > 0
			for _, p := range parts {
				if p.Leader.Host == "" {
					ready = false
				}
			}
			if ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("topic %s has no leader after 20s", topic)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

const configTemplate = `
kafka:
  brokers: ["%[1]s"]
  input_topic: "%[2]s"
  output_topic: "%[3]s"
  alert_topic: "%[4]s"
  consumer_group: "%[5]s"
windows:
  fast_window_ticks: 10
  slow_window_ticks: 100
  min_price_observations: 3
cusum:
  slack: 0.5
  threshold: 5.0
silence:
  check_interval_sec: 1
  gap_quantile: 0.999
  gap_quantile_multiplier: 1.0
  min_observations: 20
  min_threshold_ms: 1000
validation:
  enabled: true
  timestamp_tolerance_ms: 1000
alerts:
  silence_log: "%[6]s"
  validation_log: "%[7]s"
  kafka_silence_alerts: false
stats:
  report_interval_sec: 5
profiles:
  equity:
    model_key: "equity"
default_exchange:
  timezone: "UTC"
  premarket_start: "07:00"
  market_open: "09:00"
  midday_start: "11:00"
  close_start: "15:30"
  market_close: "17:30"
  afterhours_end: "20:00"
  trading_weekdays: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
`

type aggregator struct {
	cmd        *exec.Cmd
	logPath    string
	silenceLog string
	validLog   string
	done       chan error
}

func (a *aggregator) logText() string {
	b, _ := os.ReadFile(a.logPath)
	return string(b)
}

func startAggregator(t *testing.T, tp topics) *aggregator {
	t.Helper()
	bin := aggregatorBinary(t)
	dir := t.TempDir()
	a := &aggregator{
		logPath:    filepath.Join(dir, "aggregator.log"),
		silenceLog: filepath.Join(dir, "eval", "silence_alerts.csv"),
		validLog:   filepath.Join(dir, "eval", "validation_alerts.csv"),
	}
	cfgPath := filepath.Join(dir, "aggregator.yaml")
	cfg := fmt.Sprintf(configTemplate, testBroker, tp.input, tp.vectors, tp.alerts,
		fmt.Sprintf("it-%d", time.Now().UnixNano()), a.silenceLog, a.validLog)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	logFile, err := os.Create(a.logPath)
	if err != nil {
		t.Fatal(err)
	}
	a.cmd = exec.Command(bin, cfgPath)
	a.cmd.Dir = dir // the registry snapshot goes to ./data
	a.cmd.Stdout, a.cmd.Stderr = logFile, logFile
	if err := a.cmd.Start(); err != nil {
		t.Fatalf("cannot start the aggregator: %v", err)
	}
	a.done = make(chan error, 1)
	go func() { a.done <- a.cmd.Wait() }()

	deadline := time.Now().Add(40 * time.Second)
	for !strings.Contains(a.logText(), "System fully operational") {
		select {
		case err := <-a.done:
			t.Fatalf("the aggregator exited during startup (%v). Log:\n%s", err, a.logText())
		default:
		}
		if time.Now().After(deadline) {
			a.cmd.Process.Kill()
			t.Fatalf("the aggregator did not become operational in 40s. Log:\n%s", a.logText())
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Cleanup(func() {
		if a.cmd.ProcessState == nil {
			a.cmd.Process.Kill()
		}
	})
	return a
}

// stop sends SIGTERM (what `make stop-all` does) and waits for a graceful exit.
func (a *aggregator) stop(t *testing.T) {
	t.Helper()
	a.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-a.done:
	case <-time.After(20 * time.Second):
		a.cmd.Process.Kill()
		t.Errorf("the aggregator did not stop within 20s of SIGTERM. Log:\n%s", a.logText())
	}
}

func publish(t *testing.T, topic string, ticks []wireTick) {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP(testBroker), Topic: topic, Balancer: &kafka.Hash{},
		BatchTimeout: 5 * time.Millisecond, RequiredAcks: kafka.RequireAll, MaxAttempts: 10}
	defer w.Close()
	msgs := make([]kafka.Message, len(ticks))
	for i, tick := range ticks {
		b, err := json.Marshal(tick)
		if err != nil {
			t.Fatal(err)
		}
		msgs[i] = kafka.Message{Key: []byte(tick.ID), Value: b}
	}
	// Synchronous chunks of at most one batch: a partition's messages stay in order.
	for start := 0; start < len(msgs); start += 100 {
		end := start + 100
		if end > len(msgs) {
			end = len(msgs)
		}
		if err := w.WriteMessages(context.Background(), msgs[start:end]...); err != nil {
			t.Fatalf("cannot publish: %v", err)
		}
	}
}

type receivedVector struct {
	model.NormalizedVector
	key       string
	partition int
}

// readVectors reads until `want` vectors have arrived or the timeout expires, and returns
// what it got either way so a failure can report the shortfall.
func readVectors(t *testing.T, topic string, want int, timeout time.Duration) []receivedVector {
	t.Helper()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{testBroker}, Topic: topic,
		GroupID: fmt.Sprintf("it-reader-%d", time.Now().UnixNano()), StartOffset: kafka.FirstOffset, MaxWait: 200 * time.Millisecond,
	})
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []receivedVector
	for len(out) < want {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
		var v model.NormalizedVector
		if err := json.Unmarshal(m.Value, &v); err != nil {
			t.Errorf("a vector on %s is not valid JSON: %v", topic, err)
			continue
		}
		out = append(out, receivedVector{v, string(m.Key), m.Partition})
	}
	return out
}

func readCSV(t *testing.T, path string) []map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Errorf("expected log file %s: %v", path, err)
		return nil
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil || len(rows) == 0 {
		t.Errorf("cannot read %s: %v", path, err)
		return nil
	}
	var out []map[string]string
	for _, row := range rows[1:] {
		m := map[string]string{}
		for i, h := range rows[0] {
			m[h] = row[i]
		}
		out = append(out, m)
	}
	return out
}

// checklist records named checks with what was observed, and prints them all at the end,
// so one run shows everything that went right and everything that went wrong.
type checklist struct {
	t     *testing.T
	lines []string
	fails int
}

func (c *checklist) check(ok bool, name, observed string) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		c.fails++
		c.t.Errorf("%s: %s", name, observed)
	}
	c.lines = append(c.lines, fmt.Sprintf("  [%s] %-58s %s", status, name, observed))
}

func (c *checklist) report() {
	c.t.Logf("\n--- pipeline check (%d failed) ---\n%s", c.fails, strings.Join(c.lines, "\n"))
}

// ---- the scenario -------------------------------------------------------------------

func TestPipelineEndToEnd_SimulatorWireFormat(t *testing.T) {
	suffix := time.Now().UnixNano()
	tp := topics{
		input:   fmt.Sprintf("it-raw-%d", suffix),
		vectors: fmt.Sprintf("it-vectors-%d", suffix),
		alerts:  fmt.Sprintf("it-alerts-%d", suffix),
	}
	createTopics(t, tp)
	agg := startAggregator(t, tp)

	cl := &checklist{t: t}
	defer cl.report()
	defer func() {
		if t.Failed() {
			t.Logf("aggregator log:\n%s", agg.logText())
		}
	}()

	// ---- scenario --------------------------------------------------------------
	// A.ETR ticks every second for 260 s: a trade every 4th message, the rest quote
	// updates. Message 200 is an implausible trade (50x); message 220 has its TradingTime
	// rewound five minutes.
	// B.ETR ticks for 60 s, is silent for 200 s, then ticks once more.
	// C.FR ticks for 30 s on another exchange. An index row must be ignored.
	var ticks []wireTick
	price := 100.0
	trades := map[string]int{}
	var implausibleAt time.Time
	var rewoundTradingTime time.Time
	for sec := 0; sec < 260; sec++ {
		p := 0.0
		if sec%4 == 0 {
			price += 0.01 * float64(1+sec%3)
			p = price
		}
		if sec == 200 {
			p = price * 50 // implausible; keep the carried price unchanged
		}
		m := msg("A.ETR", "ETR", sec, p)
		if sec == 200 {
			implausibleAt = m.TradingTime
		}
		if sec == 220 {
			m.TradingTime = m.TradingTime.Add(-5 * time.Minute)
			rewoundTradingTime = m.TradingTime
		}
		if p > 0 && sec != 220 { // the rewound message is quarantined, so its trade never reaches the features
			trades["A.ETR"]++
		}
		ticks = append(ticks, m)

		if sec < 60 || sec == 260-1 {
			ticks = append(ticks, msg("B.ETR", "ETR", sec, 0))
		}
		if sec < 30 {
			pf := 0.0
			if sec%3 == 0 {
				pf = 50 + float64(sec)*0.001
			}
			ticks = append(ticks, msg("C.FR", "FR", sec, pf))
		}
	}
	index := msg("DAX.ETR", "ETR", 100, 15000)
	index.SecType = "I"
	ticks = append(ticks, index)

	wantVectors := 0
	for _, tick := range ticks {
		if tick.SecType == "E" && !tick.TradingTime.Equal(rewoundTradingTime) {
			wantVectors++
		}
	}

	for i := range ticks {
		ticks[i].Seq = uint64(i + 1)
	}
	seqOf := map[string]uint64{} // (instrument, TradingTime) is not unique in real data; here it is
	for _, tick := range ticks {
		seqOf[tick.ID+"@"+tick.TradingTime.Format(time.RFC3339Nano)] = tick.Seq
	}

	publish(t, tp.input, ticks)
	vectors := readVectors(t, tp.vectors, wantVectors+5, 60*time.Second)
	// give any unexpected extra vectors a moment to show up before stopping
	time.Sleep(2 * time.Second)
	agg.stop(t)

	// ---- what came out ---------------------------------------------------------
	cl.check(len(vectors) == wantVectors, "vector count (equities, minus the quarantined rewind, minus index)",
		fmt.Sprintf("got %d, want %d of %d messages sent", len(vectors), wantVectors, len(ticks)))

	byInst := map[string][]receivedVector{}
	for _, v := range vectors {
		byInst[v.Instrument] = append(byInst[v.Instrument], v)
	}
	cl.check(len(byInst["DAX.ETR"]) == 0, "index (SecType I) rows are ignored", fmt.Sprintf("%d vectors for DAX.ETR", len(byInst["DAX.ETR"])))

	// price reached the pipeline
	tradeVectors, nonZeroPrice := 0, 0
	var quoteWithPrice int
	for _, v := range byInst["A.ETR"] {
		if v.HasTrade == 1 {
			tradeVectors++
			if v.ZPriceStepFast != 0 || v.ZPriceStepSlow != 0 {
				nonZeroPrice++
			}
		} else if v.ZPriceStepFast != 0 || v.ZPriceStepSlow != 0 {
			quoteWithPrice++
		}
	}
	cl.check(tradeVectors == trades["A.ETR"], "trades are recognised (has_trade=1): the price crossed the wire",
		fmt.Sprintf("%d trade vectors, %d trades sent", tradeVectors, trades["A.ETR"]))
	cl.check(nonZeroPrice >= tradeVectors/2, "price z-scores are non-zero on trades",
		fmt.Sprintf("%d of %d trade vectors", nonZeroPrice, tradeVectors))
	cl.check(quoteWithPrice == 0, "quote rows carry no price step (z = 0)", fmt.Sprintf("%d quote vectors with a price z-score", quoteWithPrice))

	// the implausible trade stands out
	maxZ := 0.0
	for _, v := range byInst["A.ETR"] {
		if v.Timestamp.Equal(implausibleAt) {
			maxZ = math.Max(math.Abs(v.ZPriceStepFast), math.Abs(v.ZPriceStepSlow))
		}
	}
	cl.check(maxZ > 5, "the 50x price is a large price z-score", fmt.Sprintf("|z| = %.1f", maxZ))

	// the rewound message
	rewoundSeen := false
	for _, v := range byInst["A.ETR"] {
		if v.Timestamp.Equal(rewoundTradingTime) {
			rewoundSeen = true
		}
	}
	cl.check(!rewoundSeen, "the rewound-timestamp message is quarantined (no vector)", fmt.Sprintf("seen=%v", rewoundSeen))

	// the message sequence number reaches the vector
	seqOK, seqZero := 0, 0
	for _, v := range vectors {
		want := seqOf[v.Instrument+"@"+v.Timestamp.Format(time.RFC3339Nano)]
		if v.Seq == 0 {
			seqZero++
		} else if v.Seq == want {
			seqOK++
		}
	}
	cl.check(seqOK == len(vectors) && seqZero == 0, "each vector carries its message's sequence number",
		fmt.Sprintf("%d of %d match, %d without one", seqOK, len(vectors), seqZero))

	// partitioning: one key per exchange and model, one partition per key
	partitionOf := map[string]int{}
	consistent, keysOK := true, true
	for _, v := range vectors {
		if v.key != v.Exchange+":"+v.ModelKey {
			keysOK = false
		}
		if p, ok := partitionOf[v.key]; ok && p != v.partition {
			consistent = false
		}
		partitionOf[v.key] = v.partition
	}
	cl.check(keysOK, "partition key is exchange:model_key", fmt.Sprintf("keys seen: %v", keysOf(partitionOf)))
	cl.check(consistent, "each key stays on one partition", fmt.Sprintf("%v", partitionOf))

	// timing runs on the whole-second update time; the vector keeps the millisecond time
	msPrecise := 0
	for _, v := range byInst["A.ETR"] {
		if v.HasTrade == 1 && v.Timestamp.Nanosecond() == 412_000_000 {
			msPrecise++
		}
	}
	cl.check(msPrecise > 0, "vector timestamp keeps the millisecond TradingTime", fmt.Sprintf("%d trade vectors with .412", msPrecise))

	// ---- evaluation logs -------------------------------------------------------
	silence := readCSV(t, agg.silenceLog)
	var bAlerts, otherAlerts int
	var bRow map[string]string
	for _, r := range silence {
		if r["Instrument"] == "B.ETR" {
			bAlerts++
			bRow = r
		} else {
			otherAlerts++
		}
	}
	cl.check(bAlerts == 1, "B.ETR's 200 s silence is reported exactly once", fmt.Sprintf("%d alerts for B.ETR", bAlerts))
	var others []string
	for _, r := range silence {
		if r["Instrument"] != "B.ETR" {
			others = append(others, fmt.Sprintf("%s last=%s observed=%s elapsed=%sms trigger=%s", r["Instrument"], r["LastSeenMs"], r["ObservedAtMs"], r["ElapsedMs"], r["Trigger"]))
		}
	}
	cl.check(otherAlerts == 0, "no alerts for instruments that kept ticking", fmt.Sprintf("%d alerts for others %v", otherAlerts, others))
	if bRow != nil {
		lastSeen := day.Add(10*time.Hour + 59*time.Second).UnixMilli() // B's last message before the silence
		gotLast, _ := strconv.ParseInt(bRow["LastSeenMs"], 10, 64)
		detected, _ := strconv.ParseInt(bRow["DetectedAtMs"], 10, 64)
		threshold, _ := strconv.ParseFloat(bRow["ThresholdMs"], 64)
		cl.check(gotLast == lastSeen, "silence LastSeen is B's last message on the update clock",
			fmt.Sprintf("got %d, want %d", gotLast, lastSeen))
		cl.check(threshold >= 1000, "threshold is at least the clock resolution floor", fmt.Sprintf("%.0f ms", threshold))
		cl.check(detected == gotLast+int64(threshold), "detection time = LastSeen + threshold (replay-speed independent)",
			fmt.Sprintf("detected %d, last+threshold %d", detected, gotLast+int64(threshold)))
		cl.check(bRow["Trigger"] == "scan" || bRow["Trigger"] == "resume", "silence was found by the scan or on resume", bRow["Trigger"])
	}

	validation := readCSV(t, agg.validLog)
	var vdetail []string
	for i, r := range validation {
		if i < 4 {
			vdetail = append(vdetail, fmt.Sprintf("%s %s tick=%s ref=%s (%s)", r["Instrument"], r["AlertType"], r["TickTimeMs"], r["ReferenceTimeMs"], r["Detail"]))
		}
	}
	cl.check(len(validation) == 1, "exactly one validation alert (the rewound timestamp)", fmt.Sprintf("%d alerts, first: %v", len(validation), vdetail))
	if len(validation) == 1 {
		got, _ := strconv.ParseInt(validation[0]["TickTimeMs"], 10, 64)
		cl.check(validation[0]["AlertType"] == model.AlertTimestampInversion && got == rewoundTradingTime.UnixMilli(),
			"the validation alert is a timestamp inversion at the rewound time",
			fmt.Sprintf("%s at %d, want %d", validation[0]["AlertType"], got, rewoundTradingTime.UnixMilli()))
	}

	// graceful shutdown wrote the summaries
	logText := agg.logText()
	cl.check(strings.Contains(logText, "Closed silence alert log") && strings.Contains(logText, "Closed validation alert log"),
		"graceful shutdown closed both evaluation logs", "see aggregator log")
	cl.check(!strings.Contains(logText, "Pipeline error") && !bytes.Contains([]byte(logText), []byte("panic")),
		"no pipeline errors or panics in the aggregator log", "")
}

func keysOf(m map[string]int) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
