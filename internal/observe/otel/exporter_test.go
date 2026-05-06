package otel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	agenttrace "github.com/agentgate/agentgate/internal/observe/trace"
)

func TestExporterFlushesBatch(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, err := New(Options{
		Endpoint:   srv.URL,
		BatchSize:  2,
		FlushEvery: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer exp.Close(context.Background())

	now := time.Now()
	exp.Write(agenttrace.Span{
		TraceID:    "trace_1",
		StepID:     "step_1",
		StartedAt:  now,
		FinishedAt: now.Add(2 * time.Millisecond),
		Status:     "success",
		StepType:   "llm_call",
		Model:      "qwen",
	})
	exp.Write(agenttrace.Span{
		TraceID:    "trace_1",
		StepID:     "step_2",
		StartedAt:  now,
		FinishedAt: now.Add(3 * time.Millisecond),
		Status:     "error",
		ErrorMessage: "boom",
		StepType:   "llm_call",
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(bodies)
		mu.Unlock()
		if count > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) == 0 {
		t.Fatal("collector never received a batch")
	}
	var req otlpRequest
	if err := json.Unmarshal(bodies[0], &req); err != nil {
		t.Fatal(err)
	}
	if len(req.ResourceSpans) != 1 || len(req.ResourceSpans[0].ScopeSpans) != 1 {
		t.Fatal("unexpected envelope shape")
	}
	if got := len(req.ResourceSpans[0].ScopeSpans[0].Spans); got != 2 {
		t.Fatalf("expected 2 spans, got %d", got)
	}
	if req.ResourceSpans[0].ScopeSpans[0].Spans[1].Status.Code != 2 {
		t.Fatal("error span should report status code 2")
	}
}

func TestNormalizeIDStable(t *testing.T) {
	a := normalizeID("trace_abc", 32)
	b := normalizeID("trace_abc", 32)
	if a != b {
		t.Fatalf("normalizeID must be deterministic: %s vs %s", a, b)
	}
	if len(a) != 32 {
		t.Fatalf("expected 32 hex chars, got %d", len(a))
	}
}
