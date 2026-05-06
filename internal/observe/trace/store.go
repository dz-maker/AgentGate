package trace

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Span struct {
	TraceID                    string    `json:"trace_id"`
	SessionID                  string    `json:"session_id,omitempty"`
	AgentID                    string    `json:"agent_id,omitempty"`
	StepID                     string    `json:"step_id"`
	ParentStepID               string    `json:"parent_step_id,omitempty"`
	StepType                   string    `json:"step_type"`
	StartedAt                  time.Time `json:"started_at"`
	FinishedAt                 time.Time `json:"finished_at"`
	LatencyMs                  int64     `json:"latency_ms"`
	TenantID                   string    `json:"tenant_id"`
	Model                      string    `json:"model"`
	Backend                    string    `json:"backend,omitempty"`
	Instance                   string    `json:"instance,omitempty"`
	PrefixMatchTokens          int       `json:"prefix_match_tokens,omitempty"`
	PrefixMatchReason          string    `json:"prefix_match_reason,omitempty"`
	PromptTokens               int       `json:"prompt_tokens,omitempty"`
	CompletionTokens           int       `json:"completion_tokens,omitempty"`
	TotalTokens                int       `json:"total_tokens,omitempty"`
	EarlyStopFired             bool      `json:"early_stop_fired,omitempty"`
	DecodeTokensSaved          int       `json:"decode_tokens_saved,omitempty"`
	DecodeTokensSavedEstimated bool      `json:"decode_tokens_saved_estimated,omitempty"`
	DecodeTokensEstimateMethod string    `json:"decode_tokens_estimate_method,omitempty"`
	FallbackReason             string    `json:"fallback_reason,omitempty"`
	Status                     string    `json:"status"`
	ErrorMessage               string    `json:"error_message,omitempty"`
}

type Summary struct {
	TraceID                string `json:"trace_id"`
	SessionID              string `json:"session_id,omitempty"`
	AgentID                string `json:"agent_id,omitempty"`
	StartedAt              string `json:"started_at,omitempty"`
	TotalLatencyMs         int64  `json:"total_latency_ms"`
	TotalPrefixHitTokens   int    `json:"total_prefix_hit_tokens"`
	TotalDecodeTokensSaved int    `json:"total_decode_tokens_saved"`
	Spans                  []Span `json:"spans"`
}

// Sink is implemented by external systems that want a copy of every
// span — typically the OTLP exporter. Sinks are best-effort: they MUST
// NOT block the request path or return errors.
type Sink interface {
	Write(span Span)
}

// queueDepth bounds the in-flight span backlog. The goal is to keep
// disk/network IO off the request path without unbounded memory growth
// under sustained pressure. A depth of 1024 absorbs short bursts while
// staying well under 1 MB at typical span sizes.
const queueDepth = 1024

type Store struct {
	mu     sync.RWMutex
	byID   map[string][]Span
	logDir string
	sinks  []Sink

	// Async writer state. Write enqueues onto queue and returns
	// immediately; a single background goroutine drains it. Close()
	// signals the writer to drain remaining spans then exit.
	queue   chan Span
	stopCh  chan struct{}
	drained chan struct{}
	dropped atomic.Uint64
}

func NewStore(logDir string) *Store {
	s := &Store{
		byID:    map[string][]Span{},
		logDir:  logDir,
		queue:   make(chan Span, queueDepth),
		stopCh:  make(chan struct{}),
		drained: make(chan struct{}),
	}
	go s.writer()
	return s
}

// AddSink wires an additional output. Safe to call before the server
// starts taking traffic; not safe to call concurrently with Write.
func (s *Store) AddSink(sink Sink) {
	if sink == nil {
		return
	}
	s.sinks = append(s.sinks, sink)
}

func NewID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return prefix + "_unknown"
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

// Write records a span. The in-memory index is updated synchronously so
// /debug/trace/{id} sees it immediately; disk + sink writes happen on a
// background goroutine so the request path is never blocked by IO. When
// the queue is full the span is dropped and a counter incremented —
// dropping is preferable to back-pressuring real traffic.
func (s *Store) Write(span Span) error {
	s.mu.Lock()
	s.byID[span.TraceID] = append(s.byID[span.TraceID], span)
	s.mu.Unlock()

	select {
	case s.queue <- span:
	default:
		s.dropped.Add(1)
	}
	return nil
}

// Close stops accepting new disk/sink writes and drains the queue. Safe
// to call multiple times. Callers should invoke this during graceful
// shutdown after the HTTP server has stopped accepting requests.
func (s *Store) Close() {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	<-s.drained
}

// Dropped returns the number of spans that were rejected because the
// queue was full. Surfaced via /admin diagnostics for capacity tuning.
func (s *Store) Dropped() uint64 {
	return s.dropped.Load()
}

func (s *Store) writer() {
	defer close(s.drained)
	for {
		select {
		case span := <-s.queue:
			s.persist(span)
		case <-s.stopCh:
			for {
				select {
				case span := <-s.queue:
					s.persist(span)
				default:
					return
				}
			}
		}
	}
}

func (s *Store) persist(span Span) {
	for _, sink := range s.sinks {
		sink.Write(span)
	}
	if s.logDir == "" {
		return
	}
	if err := os.MkdirAll(s.logDir, 0o755); err != nil {
		return
	}
	path := filepath.Join(s.logDir, span.StartedAt.Format("2006-01-02")+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_ = json.NewEncoder(f).Encode(span)
}

func (s *Store) Get(traceID string) Summary {
	s.mu.RLock()
	spans := append([]Span(nil), s.byID[traceID]...)
	s.mu.RUnlock()

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
	return summary
}
