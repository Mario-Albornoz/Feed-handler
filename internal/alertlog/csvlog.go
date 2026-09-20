// Package alertlog writes the alert files used to evaluate the detectors: plain CSV
// with a header row and epoch-millisecond event times, so they can be joined
// directly against the simulator's ground-truth episodes.
package alertlog

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// CSVLog is an append-only CSV file that is safe for concurrent writers. Every
// row is flushed as it is written so a killed process loses nothing; alert rates
// are low compared to tick rates, so the cost is negligible.
type CSVLog struct {
	mu   sync.Mutex
	file *os.File
	w    *csv.Writer
	path string
	rows uint64
}

// Open creates (or truncates) the file at path, creating parent directories, and
// writes the header. Each run therefore starts with an empty log.
func Open(path string, header []string) (*CSVLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create directory for %s: %w", path, err)
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}

	l := &CSVLog{file: file, w: csv.NewWriter(file), path: path}
	if err := l.w.Write(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("write header to %s: %w", path, err)
	}
	l.w.Flush()
	if err := l.w.Error(); err != nil {
		file.Close()
		return nil, fmt.Errorf("write header to %s: %w", path, err)
	}
	return l, nil
}

// Write appends one row and flushes it.
func (l *CSVLog) Write(row []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.w.Write(row); err != nil {
		return err
	}
	l.w.Flush()
	if err := l.w.Error(); err != nil {
		return err
	}
	l.rows++
	return nil
}

// Rows returns the number of data rows written so far.
func (l *CSVLog) Rows() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rows
}

// Path returns the file path.
func (l *CSVLog) Path() string { return l.path }

// Close flushes and closes the file.
func (l *CSVLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.w.Flush()
	err := l.w.Error()
	if closeErr := l.file.Close(); err == nil {
		err = closeErr
	}
	return err
}

// Ms formats an event time as epoch milliseconds, or "" for the zero time.
func Ms(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconv.FormatInt(t.UnixMilli(), 10)
}
