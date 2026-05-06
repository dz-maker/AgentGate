package trace

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreWriteAndGet(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	span := Span{
		TraceID:           "trace-1",
		StepID:            "step-1",
		StartedAt:         time.Now(),
		FinishedAt:        time.Now().Add(10 * time.Millisecond),
		LatencyMs:         10,
		PrefixMatchTokens: 100,
		DecodeTokensSaved: 20,
		Status:            "success",
	}
	if err := store.Write(span); err != nil {
		t.Fatal(err)
	}

	summary := store.Get("trace-1")
	if len(summary.Spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(summary.Spans))
	}
	if summary.TotalPrefixHitTokens != 100 || summary.TotalDecodeTokensSaved != 20 {
		t.Fatalf("unexpected summary: %#v", summary)
	}

	// Disk writes happen on a background goroutine. Close drains the
	// queue so the file is guaranteed to exist before we glob.
	store.Close()

	matches, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one jsonl file, got %#v", matches)
	}
	if data, err := os.ReadFile(matches[0]); err != nil || len(data) == 0 {
		t.Fatalf("expected jsonl content, len=%d err=%v", len(data), err)
	}
}
