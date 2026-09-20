package alertlog

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func readAll(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestOpenCreatesDirectoriesAndHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "log.csv")
	l, err := Open(path, []string{"X", "Y"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	rows := readAll(t, path)
	if len(rows) != 1 || rows[0][0] != "X" || rows[0][1] != "Y" {
		t.Errorf("unexpected header: %v", rows)
	}
}

func TestWriteIsVisibleImmediately(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.csv")
	l, err := Open(path, []string{"X"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	l.Write([]string{"1"})
	// Read while the log is still open: a killed process must not lose rows.
	if rows := readAll(t, path); len(rows) != 2 || rows[1][0] != "1" {
		t.Errorf("row not flushed: %v", rows)
	}
	if l.Rows() != 1 {
		t.Errorf("Rows: got %d, want 1", l.Rows())
	}
}

func TestOpenTruncatesPreviousRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.csv")
	l, _ := Open(path, []string{"X"})
	l.Write([]string{"old"})
	l.Close()

	l, err := Open(path, []string{"X"})
	if err != nil {
		t.Fatal(err)
	}
	l.Close()

	if rows := readAll(t, path); len(rows) != 1 {
		t.Errorf("expected only the header after reopening, got %v", rows)
	}
}

func TestConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.csv")
	l, _ := Open(path, []string{"X"})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				l.Write([]string{"row"})
			}
		}()
	}
	wg.Wait()
	l.Close()

	if rows := readAll(t, path); len(rows) != 801 {
		t.Errorf("expected 801 rows, got %d", len(rows))
	}
}

func TestMs(t *testing.T) {
	if got := Ms(time.Time{}); got != "" {
		t.Errorf("zero time should be empty, got %q", got)
	}
	if got := Ms(time.UnixMilli(1636538400123)); got != "1636538400123" {
		t.Errorf("got %q", got)
	}
}
