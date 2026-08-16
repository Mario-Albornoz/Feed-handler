package stats

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ThroughputTracker tracks system performance metrics
type ThroughputTracker struct {
	ticksConsumed    atomic.Uint64
	ticksProcessed   atomic.Uint64
	vectorsPublished atomic.Uint64
	alertsPublished  atomic.Uint64
	processingErrors atomic.Uint64
	publishErrors    atomic.Uint64
	bytesRead        atomic.Uint64

	instrumentCount atomic.Uint64
	warmInstruments atomic.Uint64

	errorsByType map[string]*atomic.Uint64
	errorsMu     sync.RWMutex

	startTime      time.Time
	lastReportTime time.Time
	reportInterval time.Duration

	prevTicksConsumed    uint64
	prevTicksProcessed   uint64
	prevVectorsPublished uint64
	prevAlertsPublished  uint64
}

func NewThroughputTracker(reportInterval time.Duration) *ThroughputTracker {
	now := time.Now()
	return &ThroughputTracker{
		errorsByType:   make(map[string]*atomic.Uint64),
		startTime:      now,
		lastReportTime: now,
		reportInterval: reportInterval,
	}
}

func (t *ThroughputTracker) IncrementTicksConsumed() {
	t.ticksConsumed.Add(1)
}

func (t *ThroughputTracker) IncrementTicksProcessed() {
	t.ticksProcessed.Add(1)
}

func (t *ThroughputTracker) IncrementVectorsPublished() {
	t.vectorsPublished.Add(1)
}

func (t *ThroughputTracker) IncrementAlertsPublished() {
	t.alertsPublished.Add(1)
}

func (t *ThroughputTracker) IncrementProcessingErrors() {
	t.processingErrors.Add(1)
}

func (t *ThroughputTracker) IncrementPublishErrors() {
	t.publishErrors.Add(1)
}

func (t *ThroughputTracker) AddBytesRead(bytes uint64) {
	t.bytesRead.Add(bytes)
}

func (t *ThroughputTracker) IncrementErrorByType(errorType string) {
	t.errorsMu.Lock()
	defer t.errorsMu.Unlock()

	if counter, exists := t.errorsByType[errorType]; exists {
		counter.Add(1)
	} else {
		counter := &atomic.Uint64{}
		counter.Add(1)
		t.errorsByType[errorType] = counter
	}
}

func (t *ThroughputTracker) UpdateInstrumentStats(total, warm uint64) {
	t.instrumentCount.Store(total)
	t.warmInstruments.Store(warm)
}

func (t *ThroughputTracker) UpdateMetrics(
	ticksConsumed, ticksProcessed, vectorsPublished, alertsPublished,
	processingErrors, publishErrors uint64,
) {
	t.ticksConsumed.Store(ticksConsumed)
	t.ticksProcessed.Store(ticksProcessed)
	t.vectorsPublished.Store(vectorsPublished)
	t.alertsPublished.Store(alertsPublished)
	t.processingErrors.Store(processingErrors)
	t.publishErrors.Store(publishErrors)
}

func (t *ThroughputTracker) GetReportInterval() time.Duration {
	return t.reportInterval
}

func (t *ThroughputTracker) ShouldReport() bool {
	return time.Since(t.lastReportTime) >= t.reportInterval
}

func (t *ThroughputTracker) Report() {
	now := time.Now()
	elapsed := now.Sub(t.lastReportTime).Seconds()
	totalElapsed := now.Sub(t.startTime).Seconds()

	consumed := t.ticksConsumed.Load()
	processed := t.ticksProcessed.Load()
	vectors := t.vectorsPublished.Load()
	alerts := t.alertsPublished.Load()
	procErrors := t.processingErrors.Load()
	pubErrors := t.publishErrors.Load()
	bytes := t.bytesRead.Load()
	instruments := t.instrumentCount.Load()
	warm := t.warmInstruments.Load()

	consumeRate := float64(consumed-t.prevTicksConsumed) / elapsed
	processRate := float64(processed-t.prevTicksProcessed) / elapsed
	vectorRate := float64(vectors-t.prevVectorsPublished) / elapsed
	alertRate := float64(alerts-t.prevAlertsPublished) / elapsed

	t.prevTicksConsumed = consumed
	t.prevTicksProcessed = processed
	t.prevVectorsPublished = vectors
	t.prevAlertsPublished = alerts
	t.lastReportTime = now

	log.Printf("[INFO] ========================================")
	log.Printf("[INFO] Statistics (%ds interval):", int(t.reportInterval.Seconds()))
	log.Printf("[INFO] ========================================")
	log.Printf("[INFO]   Uptime:               %s", formatDuration(totalElapsed))
	log.Printf("[INFO]   Ticks consumed:       %s", formatNumber(consumed))
	log.Printf("[INFO]   Ticks processed:      %s", formatNumber(processed))
	log.Printf("[INFO]   Vectors published:    %s", formatNumber(vectors))
	log.Printf("[INFO]   Alerts published:     %s", formatNumber(alerts))
	log.Printf("[INFO]")
	log.Printf("[INFO]   Throughput (interval):")
	log.Printf("[INFO]     Consume rate:       %s ticks/sec", formatNumber(uint64(consumeRate)))
	log.Printf("[INFO]     Process rate:       %s ticks/sec", formatNumber(uint64(processRate)))
	log.Printf("[INFO]     Vector rate:        %s vectors/sec", formatNumber(uint64(vectorRate)))
	log.Printf("[INFO]     Alert rate:         %s alerts/sec", formatNumber(uint64(alertRate)))
	log.Printf("[INFO]")
	log.Printf("[INFO]   Instruments:")
	log.Printf("[INFO]     Total tracked:      %s", formatNumber(instruments))
	log.Printf("[INFO]     Warmed up:          %s (%.1f%%)", formatNumber(warm), percentage(warm, instruments))

	if procErrors > 0 || pubErrors > 0 {
		log.Printf("[INFO]")
		log.Printf("[INFO]   Errors:")
		log.Printf("[INFO]     Processing errors:  %s", formatNumber(procErrors))
		log.Printf("[INFO]     Publish errors:     %s", formatNumber(pubErrors))

		t.errorsMu.RLock()
		if len(t.errorsByType) > 0 {
			log.Printf("[INFO]     Error breakdown:")
			for errType, counter := range t.errorsByType {
				log.Printf("[INFO]       - %s: %s", errType, formatNumber(counter.Load()))
			}
		}
		t.errorsMu.RUnlock()
	}

	if bytes > 0 {
		log.Printf("[INFO]")
		log.Printf("[INFO]   Data processed:      %s", formatBytes(bytes))
	}

	log.Printf("[INFO] ========================================")
}

func formatNumber(n uint64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result []byte
	mod := len(str) % 3
	if mod > 0 {
		result = append(result, str[:mod]...)
		if len(str) > mod {
			result = append(result, ',')
		}
	}

	for i := mod; i < len(str); i += 3 {
		result = append(result, str[i:i+3]...)
		if i+3 < len(str) {
			result = append(result, ',')
		}
	}

	return string(result)
}

func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp])
}

func formatDuration(seconds float64) string {
	d := time.Duration(seconds * float64(time.Second))

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	secs := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, secs)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, secs)
	}
	return fmt.Sprintf("%ds", secs)
}

func percentage(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 0.0
	}
	return float64(numerator) / float64(denominator) * 100.0
}
