package trace

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Replay reconstructs a trace from on-disk JSONL spans. The in-memory
// store only keeps spans for the lifetime of the process, so any
// /debug/trace lookup for a trace from a previous run had to either fail
// or come from disk. This loader is the disk path.
//
// Replay scans every JSONL file in logDir whose date prefix is within the
// look-back window. We deliberately scan rather than maintain an index:
// the file count is small (one per day), and replay is a debug operation,
// not a hot path.
//
// The returned Summary uses the same shape /debug/trace/{trace_id}
// emits, so the front-end (or curl) can read either source identically.
type Replay struct {
	logDir string
}

func NewReplay(logDir string) *Replay {
	return &Replay{logDir: logDir}
}

// Lookup returns spans for traceID. Returns nil if the directory is unset
// or no spans match. lookbackDays caps how far back we scan so a busy
// gateway with months of logs does not pay an O(everything) cost.
func (r *Replay) Lookup(traceID string, lookbackDays int) (Summary, error) {
	if r.logDir == "" {
		return Summary{}, errors.New("trace log dir not configured")
	}
	if lookbackDays <= 0 {
		lookbackDays = 7
	}
	files, err := os.ReadDir(r.logDir)
	if err != nil {
		return Summary{}, err
	}
	cutoff := time.Now().AddDate(0, 0, -lookbackDays)

	var spans []Span
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if filepath.Ext(name) != ".jsonl" {
			continue
		}
		datePart := name[:len(name)-len(".jsonl")]
		if t, err := time.Parse("2006-01-02", datePart); err == nil && t.Before(cutoff) {
			continue
		}
		path := filepath.Join(r.logDir, name)
		matched, err := scanFile(path, traceID)
		if err != nil {
			return Summary{}, err
		}
		spans = append(spans, matched...)
	}
	if len(spans) == 0 {
		return Summary{}, ErrTraceNotFound
	}
	sort.SliceStable(spans, func(i, j int) bool {
		return spans[i].StartedAt.Before(spans[j].StartedAt)
	})
	summary := Summary{TraceID: traceID, Spans: spans}
	for i, span := range spans {
		if i == 0 {
			summary.SessionID = span.SessionID
			summary.AgentID = span.AgentID
			summary.StartedAt = span.StartedAt.Format(time.RFC3339Nano)
		}
		summary.TotalLatencyMs += span.LatencyMs
		summary.TotalPrefixHitTokens += span.PrefixMatchTokens
		summary.TotalDecodeTokensSaved += span.DecodeTokensSaved
	}
	return summary, nil
}

// ErrTraceNotFound is returned when no JSONL line matches.
var ErrTraceNotFound = errors.New("trace not found")

func scanFile(path, traceID string) ([]Span, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var out []Span
	for scanner.Scan() {
		line := scanner.Bytes()
		// cheap pre-filter: if the trace ID isn't in the line, skip JSON
		// parsing. JSONL files can be hundreds of MB.
		if !containsBytes(line, traceID) {
			continue
		}
		var span Span
		if err := json.Unmarshal(line, &span); err != nil {
			continue
		}
		if span.TraceID == traceID {
			out = append(out, span)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func containsBytes(haystack []byte, needle string) bool {
	if needle == "" {
		return true
	}
	n := len(needle)
	if n > len(haystack) {
		return false
	}
	first := needle[0]
	for i := 0; i+n <= len(haystack); i++ {
		if haystack[i] != first {
			continue
		}
		match := true
		for j := 1; j < n; j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
