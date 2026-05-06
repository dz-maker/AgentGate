package otel

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"time"

	agenttrace "github.com/agentgate/agentgate/internal/observe/trace"
)

// Exporter pushes Agent Trace spans to an OTLP-HTTP /v1/traces endpoint.
//
// It is a Sink in the trace.Store sense: when configured, every span the
// gateway records is also enqueued and shipped in batches. Failures are
// logged via the optional ErrorFn and never block the request path.
type Exporter struct {
	endpoint    string
	headers     map[string]string
	serviceName string
	client      *http.Client
	timeout     time.Duration

	mu      sync.Mutex
	pending []agenttrace.Span
	flushAt time.Time

	batchSize  int
	flushEvery time.Duration

	errorFn func(error)

	stop chan struct{}
}

type Options struct {
	Endpoint    string // e.g. https://otel.example.com/v1/traces
	Headers     map[string]string
	ServiceName string
	BatchSize   int
	FlushEvery  time.Duration
	Timeout     time.Duration
	ErrorFn     func(error)
}

func New(opts Options) (*Exporter, error) {
	if opts.Endpoint == "" {
		return nil, fmt.Errorf("otel endpoint is required")
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 64
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 5 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.ServiceName == "" {
		opts.ServiceName = "agentgate"
	}
	exp := &Exporter{
		endpoint:    opts.Endpoint,
		headers:     opts.Headers,
		serviceName: opts.ServiceName,
		client:      &http.Client{Timeout: opts.Timeout},
		timeout:     opts.Timeout,
		batchSize:   opts.BatchSize,
		flushEvery:  opts.FlushEvery,
		errorFn:     opts.ErrorFn,
		stop:        make(chan struct{}),
	}
	go exp.loop()
	return exp, nil
}

// Write enqueues a span. Implements agenttrace.Sink.
func (e *Exporter) Write(span agenttrace.Span) {
	e.mu.Lock()
	e.pending = append(e.pending, span)
	full := len(e.pending) >= e.batchSize
	e.mu.Unlock()
	if full {
		go e.flushOnce(context.Background())
	}
}

func (e *Exporter) Close(ctx context.Context) error {
	close(e.stop)
	return e.flushOnce(ctx)
}

func (e *Exporter) loop() {
	ticker := time.NewTicker(e.flushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = e.flushOnce(context.Background())
		case <-e.stop:
			return
		}
	}
}

func (e *Exporter) flushOnce(ctx context.Context) error {
	e.mu.Lock()
	if len(e.pending) == 0 {
		e.mu.Unlock()
		return nil
	}
	batch := e.pending
	e.pending = nil
	e.mu.Unlock()

	body, err := json.Marshal(toOTLPRequest(e.serviceName, batch))
	if err != nil {
		e.report(err)
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
	if err != nil {
		e.report(err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.headers {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.report(err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		err := fmt.Errorf("otel collector returned %d", resp.StatusCode)
		e.report(err)
		return err
	}
	return nil
}

func (e *Exporter) report(err error) {
	if e.errorFn != nil {
		e.errorFn(err)
	}
}

// --- OTLP JSON model (only the fields we actually emit) ---

type otlpRequest struct {
	ResourceSpans []resourceSpans `json:"resourceSpans"`
}

type resourceSpans struct {
	Resource   resource     `json:"resource"`
	ScopeSpans []scopeSpans `json:"scopeSpans"`
}

type resource struct {
	Attributes []kv `json:"attributes"`
}

type scopeSpans struct {
	Scope scope    `json:"scope"`
	Spans []otSpan `json:"spans"`
}

type scope struct {
	Name string `json:"name"`
}

type otSpan struct {
	TraceID           string `json:"traceId"`
	SpanID            string `json:"spanId"`
	ParentSpanID      string `json:"parentSpanId,omitempty"`
	Name              string `json:"name"`
	Kind              int    `json:"kind"`
	StartTimeUnixNano string `json:"startTimeUnixNano"`
	EndTimeUnixNano   string `json:"endTimeUnixNano"`
	Attributes        []kv   `json:"attributes,omitempty"`
	Status            otStat `json:"status,omitempty"`
}

type otStat struct {
	Code int `json:"code"`
}

type kv struct {
	Key   string  `json:"key"`
	Value otValue `json:"value"`
}

type otValue struct {
	StringValue *string `json:"stringValue,omitempty"`
	IntValue    *int64  `json:"intValue,omitempty"`
	BoolValue   *bool   `json:"boolValue,omitempty"`
}

func toOTLPRequest(service string, spans []agenttrace.Span) otlpRequest {
	out := make([]otSpan, 0, len(spans))
	for _, s := range spans {
		statusCode := 1 // OK
		if s.Status == "error" {
			statusCode = 2
		}
		out = append(out, otSpan{
			TraceID:           normalizeID(s.TraceID, 32),
			SpanID:            normalizeID(s.StepID, 16),
			ParentSpanID:      normalizeIDOrEmpty(s.ParentStepID, 16),
			Name:              defaultStr(s.StepType, "agent.step"),
			Kind:              3, // CLIENT
			StartTimeUnixNano: fmt.Sprintf("%d", s.StartedAt.UnixNano()),
			EndTimeUnixNano:   fmt.Sprintf("%d", s.FinishedAt.UnixNano()),
			Attributes:        spanAttributes(s),
			Status:            otStat{Code: statusCode},
		})
	}
	return otlpRequest{ResourceSpans: []resourceSpans{{
		Resource: resource{Attributes: []kv{
			strKV("service.name", service),
		}},
		ScopeSpans: []scopeSpans{{
			Scope: scope{Name: "agentgate"},
			Spans: out,
		}},
	}}}
}

func spanAttributes(s agenttrace.Span) []kv {
	attrs := []kv{}
	if s.SessionID != "" {
		attrs = append(attrs, strKV("agent.session_id", s.SessionID))
	}
	if s.AgentID != "" {
		attrs = append(attrs, strKV("agent.id", s.AgentID))
	}
	if s.TenantID != "" {
		attrs = append(attrs, strKV("agent.tenant", s.TenantID))
	}
	if s.Backend != "" {
		attrs = append(attrs, strKV("backend.name", s.Backend))
	}
	if s.Instance != "" {
		attrs = append(attrs, strKV("backend.instance", s.Instance))
	}
	if s.Model != "" {
		attrs = append(attrs, strKV("llm.model", s.Model))
	}
	if s.PrefixMatchTokens > 0 {
		attrs = append(attrs, intKV("agent.prefix_match_tokens", int64(s.PrefixMatchTokens)))
	}
	if s.PrefixMatchReason != "" {
		attrs = append(attrs, strKV("agent.prefix_match_reason", s.PrefixMatchReason))
	}
	if s.PromptTokens > 0 {
		attrs = append(attrs, intKV("llm.prompt_tokens", int64(s.PromptTokens)))
	}
	if s.CompletionTokens > 0 {
		attrs = append(attrs, intKV("llm.completion_tokens", int64(s.CompletionTokens)))
	}
	if s.EarlyStopFired {
		t := true
		attrs = append(attrs, kv{Key: "agent.early_stop_fired", Value: otValue{BoolValue: &t}})
		if s.DecodeTokensSaved > 0 {
			attrs = append(attrs, intKV("agent.decode_tokens_saved", int64(s.DecodeTokensSaved)))
		}
	}
	if s.FallbackReason != "" {
		attrs = append(attrs, strKV("agent.fallback_reason", s.FallbackReason))
	}
	if s.ErrorMessage != "" {
		attrs = append(attrs, strKV("error.message", s.ErrorMessage))
	}
	return attrs
}

func strKV(k, v string) kv  { return kv{Key: k, Value: otValue{StringValue: &v}} }
func intKV(k string, v int64) kv {
	return kv{Key: k, Value: otValue{IntValue: &v}}
}

func defaultStr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// normalizeID hashes our string trace/span IDs into the 16/32-hex format
// OTLP expects. We do this deterministically (FNV-64a) so the same trace
// always maps to the same OTLP traceId across exports.
func normalizeID(id string, hexLen int) string {
	if id == "" {
		// Use random-ish from time to satisfy the format.
		return hex.EncodeToString(make([]byte, hexLen/2))
	}
	bytesNeeded := hexLen / 2
	out := make([]byte, 0, bytesNeeded)
	seed := id
	for len(out) < bytesNeeded {
		h := fnv.New64a()
		_, _ = h.Write([]byte(seed))
		out = append(out, h.Sum(nil)...)
		seed = "rehash:" + seed
	}
	return hex.EncodeToString(out[:bytesNeeded])
}

func normalizeIDOrEmpty(id string, hexLen int) string {
	if id == "" {
		return ""
	}
	return normalizeID(id, hexLen)
}
