package vllm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestCompleteRemovesAgentGateExtensionAndInjectsStops(t *testing.T) {
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(types.Response{
			ID:     "chatcmpl_test",
			Object: "chat.completion",
			Model:  "qwen",
			Choices: []types.Choice{{
				Index: 0,
				Message: types.Message{
					Role:    types.RoleAssistant,
					Content: "ok",
				},
				FinishReason: "stop",
			}},
		})
	}))
	defer server.Close()

	adapter, err := New(Options{Name: "vllm-test", Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatal(err)
	}

	raw := []byte(`{"model":"qwen","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"search"}}],"x_agentgate":{"session_id":"s1"},"stop":"END"}`)
	req := types.Request{
		Model: "qwen",
		Tools: []types.ToolDefinition{{Type: "function", Function: []byte(`{"name":"search"}`)}},
		Raw:   raw,
	}
	resp, err := adapter.Complete(context.Background(), &req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ID != "chatcmpl_test" {
		t.Fatalf("unexpected response id %q", resp.ID)
	}
	if _, ok := payload["x_agentgate"]; ok {
		t.Fatal("x_agentgate should not be forwarded to vLLM")
	}
	stops, ok := payload["stop"].([]any)
	if !ok {
		t.Fatalf("expected stop list, got %#v", payload["stop"])
	}
	hasToolStop := false
	hasOriginalStop := false
	for _, item := range stops {
		switch item {
		case "</tool_call>":
			hasToolStop = true
		case "END":
			hasOriginalStop = true
		}
	}
	if !hasToolStop || !hasOriginalStop {
		t.Fatalf("expected original and tool stop strings, got %#v", stops)
	}
}

func TestStreamParsesSSEChunks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter, err := New(Options{Name: "vllm-test", Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Stream(context.Background(), &types.Request{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	var chunks []types.Chunk
	for chunk := range stream {
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	if chunks[0].Content != "hel" || chunks[1].Content != "lo" || chunks[1].FinishReason != "stop" {
		t.Fatalf("unexpected chunks: %#v", chunks)
	}
}

func TestInstanceHealthUsesFailureAndRecoveryThresholds(t *testing.T) {
	inst := &Instance{}
	inst.healthy.Store(true)

	inst.fail(context.DeadlineExceeded)
	inst.fail(context.DeadlineExceeded)
	if !inst.isHealthy() {
		t.Fatal("instance should stay healthy until failure threshold")
	}

	inst.fail(context.DeadlineExceeded)
	if inst.isHealthy() {
		t.Fatal("instance should be unhealthy after threshold failures")
	}

	inst.success()
	if inst.isHealthy() {
		t.Fatal("instance should need two successes to recover")
	}

	inst.success()
	if !inst.isHealthy() {
		t.Fatal("instance should recover after consecutive successes")
	}
}

func TestStreamContextCanceledIsTreatedAsSuccessfulGatewayCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\n"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	adapter, err := New(Options{Name: "vllm-test", Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := adapter.Stream(ctx, &types.Request{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-stream:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first stream chunk")
	}
	cancel()
	for range stream {
	}

	stats := adapter.Stats()
	if stats.FailedRequests != 0 {
		t.Fatalf("controlled cancel should not count as failed request: %#v", stats)
	}
	if stats.InFlight != 0 {
		t.Fatalf("expected no in-flight requests after cancel: %#v", stats)
	}
	if !stats.Healthy {
		t.Fatalf("controlled cancel should keep backend healthy: %#v", stats)
	}
}

func TestVLLMRespectsRequestToolChoice(t *testing.T) {
	// vLLM speaks the OpenAI tool_choice protocol, so the gateway must
	// pass through caller-specified values like "required" or a forced
	// function name. Hardcoding "auto" silently downgrades policies that
	// rely on forced tool invocation.
	var seen map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&seen)
		_ = json.NewEncoder(w).Encode(types.Response{Object: "chat.completion"})
	}))
	defer server.Close()

	adapter, err := New(Options{Name: "vllm-tc", Endpoints: []string{server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	req := types.Request{
		Model:      "qwen",
		Messages:   []types.Message{{Role: types.RoleUser, Content: "hi"}},
		Tools:      []types.ToolDefinition{{Type: "function", Function: []byte(`{"name":"search"}`)}},
		ToolChoice: "required",
	}
	if _, err := adapter.Complete(context.Background(), &req); err != nil {
		t.Fatal(err)
	}
	if got := seen["tool_choice"]; got != "required" {
		t.Fatalf("tool_choice not propagated, got %#v", got)
	}
}

func TestVLLMHealthCheckPropagatesHeaders(t *testing.T) {
	// vLLM clusters behind auth proxies (DiDi gateway, k8s NetworkPolicy)
	// will reject probes that don't carry the configured Authorization
	// header. Without it the gateway permanently marks the instance
	// unhealthy and load balancing collapses.
	got := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			got <- r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			return
		}
	}))
	defer server.Close()

	adapter, err := New(Options{
		Name:           "vllm-auth",
		Endpoints:      []string{server.URL},
		Headers:        map[string]string{"Authorization": "Bearer secret"},
		HealthInterval: 20 * time.Millisecond,
		HealthTimeout:  500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()

	select {
	case auth := <-got:
		if auth != "Bearer secret" {
			t.Fatalf("health probe missing auth header, got %q", auth)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("health probe never fired or never sent the header")
	}
}
