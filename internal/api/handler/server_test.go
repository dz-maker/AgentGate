package handler

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agentgate/agentgate/internal/backend"
	"github.com/agentgate/agentgate/internal/backend/mock"
	"github.com/agentgate/agentgate/internal/cache/prefix"
	"github.com/agentgate/agentgate/internal/cache/semantic"
	"github.com/agentgate/agentgate/internal/fallback"
	agenttrace "github.com/agentgate/agentgate/internal/observe/trace"
	"github.com/agentgate/agentgate/internal/policy"
	"github.com/agentgate/agentgate/internal/router"
	"github.com/agentgate/agentgate/pkg/types"
)

type handlerTestBackend struct {
	name      string
	vendor    string
	text      string
	err       error
	delay     time.Duration
	streamUse *types.Usage
	calls     atomic.Int32
}

func (b *handlerTestBackend) Name() string { return b.name }

func (b *handlerTestBackend) Complete(ctx context.Context, req *types.Request) (*types.Response, error) {
	b.calls.Add(1)
	if b.delay > 0 {
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if b.err != nil {
		return nil, b.err
	}
	return &types.Response{
		ID:      "chatcmpl_" + b.name,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []types.Choice{{
			Index:        0,
			Message:      types.Message{Role: types.RoleAssistant, Content: b.text},
			FinishReason: "stop",
		}},
		Usage: &types.Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3},
	}, nil
}

func (b *handlerTestBackend) Stream(ctx context.Context, req *types.Request) (<-chan types.Chunk, error) {
	b.calls.Add(1)
	if b.err != nil {
		return nil, b.err
	}
	out := make(chan types.Chunk, 2)
	go func() {
		defer close(out)
		select {
		case out <- types.Chunk{ID: "chatcmpl_" + b.name, Model: req.Model, Content: b.text, CreatedAt: time.Now()}:
		case <-ctx.Done():
			return
		}
		select {
		case out <- types.Chunk{ID: "chatcmpl_" + b.name, Model: req.Model, FinishReason: "stop", Usage: b.streamUse, CreatedAt: time.Now()}:
		case <-ctx.Done():
			return
		}
	}()
	return out, nil
}

func (b *handlerTestBackend) Capabilities() types.Capabilities {
	return types.Capabilities{Vendor: b.vendor, SupportsStreaming: true}
}
func (b *handlerTestBackend) Healthy() bool { return true }
func (b *handlerTestBackend) Stats() types.BackendStats {
	return types.BackendStats{Name: b.name, Healthy: true}
}

func TestChatCompletionWithMockBackend(t *testing.T) {
	mockBackend := mock.New("mock")
	registry := backend.NewRegistry([]backend.Backend{mockBackend})
	prefixSvc := prefix.NewService(prefix.Options{MaxEntries: 100})
	traceStore := agenttrace.NewStore("")
	srv := New(Options{
		Router:          router.New(registry, prefixSvc),
		Registry:        registry,
		Prefix:          prefixSvc,
		TraceStore:      traceStore,
		EnableToolParse: true,
		ParserBuffer:    4096,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
		"model":"mock",
		"messages":[{"role":"system","content":"be terse"},{"role":"user","content":"hello"}]
	}`))
	rec := httptest.NewRecorder()

	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "mock backend is running") {
		t.Fatalf("unexpected body %s", rec.Body.String())
	}
	traceID := rec.Header().Get("X-AgentGate-Trace-Id")
	if traceID == "" {
		t.Fatal("trace header missing")
	}

	traceReq := httptest.NewRequest(http.MethodGet, "/debug/trace/"+traceID, nil)
	traceReq.SetPathValue("trace_id", traceID)
	traceRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(traceRec, traceReq)
	if traceRec.Code != http.StatusOK {
		t.Fatalf("trace status %d: %s", traceRec.Code, traceRec.Body.String())
	}
}

func TestStreamingChatCompletionWithMockBackend(t *testing.T) {
	mockBackend := mock.New("mock")
	registry := backend.NewRegistry([]backend.Backend{mockBackend})
	prefixSvc := prefix.NewService(prefix.Options{MaxEntries: 100})
	srv := New(Options{
		Router:          router.New(registry, prefixSvc),
		Registry:        registry,
		Prefix:          prefixSvc,
		EnableToolParse: true,
		ParserBuffer:    4096,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
		"model":"mock",
		"stream":true,
		"messages":[{"role":"user","content":"hello"}]
	}`))
	rec := httptest.NewRecorder()

	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("unexpected content-type %q", ct)
	}

	scanner := bufio.NewScanner(bytes.NewReader(rec.Body.Bytes()))
	sawDone := false
	for scanner.Scan() {
		if scanner.Text() == "data: [DONE]" {
			sawDone = true
		}
	}
	if !sawDone {
		t.Fatalf("stream did not end with DONE: %s", rec.Body.String())
	}
}

func TestPolicyRoutingSelectsConfiguredBackend(t *testing.T) {
	defaultBackend := &handlerTestBackend{name: "default", vendor: "mock", text: "wrong"}
	targetBackend := &handlerTestBackend{name: "target", vendor: "vllm", text: "routed"}
	registry := backend.NewRegistry([]backend.Backend{defaultBackend, targetBackend})
	policyEngine := policy.New(policy.Document{Routing: []policy.RoutingRule{
		{Name: "premium-to-target", When: policy.Match{Tenant: "premium"}, Backend: "target"},
	}})
	srv := New(Options{
		Router:   router.New(registry, nil),
		Registry: registry,
		Policy:   policyEngine,
		Breakers: fallback.NewSet(fallback.Options{FailureThreshold: 1}),
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
		"model":"m",
		"messages":[{"role":"user","content":"hello"}],
		"x_agentgate":{"tenant_id":"premium"}
	}`))
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "routed") {
		t.Fatalf("policy route did not hit target backend: %s", rec.Body.String())
	}
	if defaultBackend.calls.Load() != 0 || targetBackend.calls.Load() != 1 {
		t.Fatalf("unexpected backend calls default=%d target=%d", defaultBackend.calls.Load(), targetBackend.calls.Load())
	}
	if got := rec.Header().Get("X-AgentGate-Policy-Rule"); got != "premium-to-target" {
		t.Fatalf("policy rule header missing: %q", got)
	}
}

func TestPolicyFallbackChainFallsThroughOnBackendError(t *testing.T) {
	primary := &handlerTestBackend{name: "primary", vendor: "vllm", err: errors.New("boom")}
	secondary := &handlerTestBackend{name: "secondary", vendor: "openai", text: "fallback ok"}
	registry := backend.NewRegistry([]backend.Backend{primary, secondary})
	policyEngine := policy.New(policy.Document{Routing: []policy.RoutingRule{
		{Name: "with-fallback", Backend: "primary", Fallback: []string{"secondary"}},
	}})
	srv := New(Options{
		Router:   router.New(registry, nil),
		Registry: registry,
		Policy:   policyEngine,
		Breakers: fallback.NewSet(fallback.Options{FailureThreshold: 1}),
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hello"}]}`))
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "fallback ok") {
		t.Fatalf("fallback did not serve response: %s", rec.Body.String())
	}
	if got := rec.Header().Get("X-AgentGate-Backend"); got != "secondary" {
		t.Fatalf("expected secondary backend header, got %q", got)
	}
}

func TestVendorBudgetBlocksBeforeBackendCall(t *testing.T) {
	anthropicBackend := &handlerTestBackend{name: "anthropic", vendor: "anthropic", text: "expensive"}
	registry := backend.NewRegistry([]backend.Backend{anthropicBackend})
	policyEngine := policy.New(policy.Document{Budgets: []policy.BudgetRule{
		{Name: "anthropic-cap", When: policy.Match{Vendor: "anthropic"}, Window: time.Hour, MaxTokens: 1, Action: "deny"},
	}})
	policyEngine.AccountUsage(types.Request{TenantID: "t"}, "anthropic", 2, 0)
	srv := New(Options{
		Router:   router.New(registry, nil),
		Registry: registry,
		Policy:   policyEngine,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
		"model":"claude",
		"messages":[{"role":"user","content":"hello"}],
		"x_agentgate":{"tenant_id":"t"}
	}`))
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d: %s", rec.Code, rec.Body.String())
	}
	if anthropicBackend.calls.Load() != 0 {
		t.Fatalf("budget denial should happen before backend call, got %d calls", anthropicBackend.calls.Load())
	}
}

func TestSemanticSingleflightIsUsedByHandler(t *testing.T) {
	slow := &handlerTestBackend{name: "slow", vendor: "vllm", text: "once", delay: 50 * time.Millisecond}
	registry := backend.NewRegistry([]backend.Backend{slow})
	srv := New(Options{
		Router:   router.New(registry, nil),
		Registry: registry,
		Semantic: semantic.New(semantic.Options{}),
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"same"}]}`

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("status %d: %s", rec.Code, rec.Body.String())
			}
		}()
	}
	close(start)
	wg.Wait()

	if calls := slow.calls.Load(); calls != 1 {
		t.Fatalf("singleflight should collapse backend calls to 1, got %d", calls)
	}
}

func TestDefaultRoutingUsesCostModelWhenNoPolicyMatches(t *testing.T) {
	expensive := &handlerTestBackend{name: "expensive", vendor: "openai", text: "wrong"}
	cheap := &handlerTestBackend{name: "cheap", vendor: "openai", text: "cheap path"}
	registry := backend.NewRegistry([]backend.Backend{expensive, cheap})
	costModel := router.NewCostModel()
	costModel.Observe("expensive", 1000, 900*time.Millisecond)
	costModel.Observe("cheap", 1000, 50*time.Millisecond)
	srv := New(Options{
		Router:   router.New(registry, nil),
		Registry: registry,
		Cost:     costModel,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hello"}]}`))
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-AgentGate-Backend"); got != "cheap" {
		t.Fatalf("expected cost-aware route to cheap backend, got %q", got)
	}
	if expensive.calls.Load() != 0 || cheap.calls.Load() != 1 {
		t.Fatalf("unexpected calls expensive=%d cheap=%d", expensive.calls.Load(), cheap.calls.Load())
	}
}

func TestStreamingUsageFeedsVendorBudget(t *testing.T) {
	streamBackend := &handlerTestBackend{
		name:      "anthropic",
		vendor:    "anthropic",
		text:      "hello",
		streamUse: &types.Usage{PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5},
	}
	registry := backend.NewRegistry([]backend.Backend{streamBackend})
	policyEngine := policy.New(policy.Document{Budgets: []policy.BudgetRule{
		{Name: "anthropic-cap", When: policy.Match{Vendor: "anthropic"}, Window: time.Hour, MaxTokens: 4, Action: "deny"},
	}})
	srv := New(Options{
		Router:   router.New(registry, nil),
		Registry: registry,
		Policy:   policyEngine,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	body := `{
		"model":"claude",
		"stream":true,
		"messages":[{"role":"user","content":"hello"}],
		"x_agentgate":{"tenant_id":"t"}
	}`

	first := httptest.NewRecorder()
	srv.Routes().ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if first.Code != http.StatusOK {
		t.Fatalf("first stream status %d: %s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	srv.Routes().ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request to hit stream-accounted budget, got %d: %s", second.Code, second.Body.String())
	}
}

func TestEstimateDecodeTokensSavedIsCappedAndExplicitlyEstimated(t *testing.T) {
	maxTokens := 4096
	got := estimateDecodeTokensSaved(&maxTokens, 20)
	if got != maxToolEarlyStopSavedEstimate {
		t.Fatalf("expected capped estimate %d, got %d", maxToolEarlyStopSavedEstimate, got)
	}

	maxTokens = 30
	got = estimateDecodeTokensSaved(&maxTokens, 20)
	if got != 10 {
		t.Fatalf("expected remaining budget estimate 10, got %d", got)
	}

	got = estimateDecodeTokensSaved(nil, 20)
	if got != defaultToolEarlyStopSavedEstimate {
		t.Fatalf("expected default estimate, got %d", got)
	}
}

func TestWriteJSONHandlesEncodeError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, map[string]float64{"bad": math.Inf(1)})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for unencodable JSON, got %d", rec.Code)
	}
}
