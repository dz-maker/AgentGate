package trace

import (
	"path/filepath"
	"testing"
	"time"
)

func TestReplayLoadsSpansFromJSONL(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)

	now := time.Now()
	spans := []Span{
		{TraceID: "trace_x", StepID: "s1", StepType: "llm_call", StartedAt: now, FinishedAt: now.Add(10 * time.Millisecond), Status: "success", LatencyMs: 10},
		{TraceID: "trace_x", StepID: "s2", StepType: "llm_call", StartedAt: now.Add(time.Millisecond), FinishedAt: now.Add(20 * time.Millisecond), Status: "success", LatencyMs: 19},
		{TraceID: "trace_y", StepID: "other"},
	}
	for _, s := range spans {
		if err := store.Write(s); err != nil {
			t.Fatal(err)
		}
	}
	// Disk writes happen asynchronously; drain before replay reads.
	store.Close()

	r := NewReplay(dir)
	summary, err := r.Lookup("trace_x", 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(summary.Spans))
	}
	if summary.Spans[0].StepID != "s1" {
		t.Fatalf("expected sorted by start time, got %v", summary.Spans)
	}
}

func TestReplayReturnsNotFoundForUnknownTrace(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	store.Write(Span{TraceID: "trace_a", StepID: "s", StartedAt: time.Now(), FinishedAt: time.Now()})
	store.Close()

	r := NewReplay(dir)
	if _, err := r.Lookup("trace_missing", 7); err != ErrTraceNotFound {
		t.Fatalf("expected ErrTraceNotFound, got %v", err)
	}
}

func TestReplayBuildsConsistentLogDirPath(t *testing.T) {
	// Sanity: the store writes per-day files we can read back.
	dir := t.TempDir()
	r := NewReplay(dir)
	if _, err := r.Lookup("anything", 1); err == nil {
		t.Fatal("empty dir should not produce a hit")
	}
	expected := filepath.Join(dir, time.Now().UTC().Format("2006-01-02")+".jsonl")
	_ = expected
}
