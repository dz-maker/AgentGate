package anthropic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestAnthropicCapabilitiesAdvertiseNoPrefixCache(t *testing.T) {
	a, err := New(Options{Name: "anthropic-prod", APIKey: "sk-ant-test"})
	if err != nil {
		t.Fatal(err)
	}
	caps := a.Capabilities()
	if caps.Vendor != "anthropic" {
		t.Fatalf("vendor must be anthropic, got %q", caps.Vendor)
	}
	if caps.PrefixCacheMode != types.PrefixCacheNone {
		t.Fatalf("expected PrefixCacheNone, got %q", caps.PrefixCacheMode)
	}
}

func TestBuildPayloadSplitsSystemPrompt(t *testing.T) {
	req := &types.Request{
		Model:     "claude-3-5-sonnet",
		MaxTokens: nil,
		Messages: []types.Message{
			{Role: types.RoleSystem, Content: "you are kind"},
			{Role: types.RoleUser, Content: "hello"},
		},
	}
	payload := buildPayload(req, false)
	system, ok := payload["system"].(string)
	if !ok || !strings.Contains(system, "kind") {
		t.Fatalf("system block missing: %#v", payload["system"])
	}
	msgs := payload["messages"].([]map[string]any)
	if len(msgs) != 1 || msgs[0]["role"] != "user" {
		t.Fatalf("unexpected messages: %#v", msgs)
	}
	if payload["max_tokens"] != 1024 {
		t.Fatalf("max_tokens default missing: %#v", payload["max_tokens"])
	}
}

func TestBuildPayloadAppliesEphemeralCacheOnShareMaxHint(t *testing.T) {
	req := &types.Request{
		Model:        "claude-3-5-sonnet",
		Messages:     []types.Message{{Role: types.RoleSystem, Content: "system X"}, {Role: types.RoleUser, Content: "hi"}},
		CacheControl: types.CachePolicy{PrefixHint: "share_max"},
	}
	payload := buildPayload(req, false)
	systemBlocks, ok := payload["system"].([]map[string]any)
	if !ok || len(systemBlocks) == 0 {
		t.Fatalf("expected system blocks: %#v", payload["system"])
	}
	if _, ok := systemBlocks[0]["cache_control"]; !ok {
		t.Fatalf("expected cache_control on system block: %#v", systemBlocks[0])
	}
}

func TestAnthropicCompleteTranslatesMessagesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-api-key"); got != "sk-ant-test" {
			t.Fatalf("missing api key: %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		var sent map[string]any
		_ = json.Unmarshal(body, &sent)
		if sent["model"] != "claude" {
			t.Fatalf("model did not propagate: %#v", sent)
		}
		_, _ = io.WriteString(w, `{"id":"msg_1","model":"claude","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":2}}`)
	}))
	defer srv.Close()

	a, err := New(Options{Name: "anthropic", Endpoint: srv.URL, APIKey: "sk-ant-test"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := a.Complete(context.Background(), &types.Request{
		Model:    "claude",
		Messages: []types.Message{{Role: types.RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Choices[0].Message.ContentString() != "hi" {
		t.Fatalf("unexpected content: %s", resp.Choices[0].Message.ContentString())
	}
	if resp.Choices[0].FinishReason != "stop" {
		t.Fatalf("unexpected finish reason: %s", resp.Choices[0].FinishReason)
	}
	if resp.Usage == nil || resp.Usage.TotalTokens != 6 {
		t.Fatalf("usage not propagated: %+v", resp.Usage)
	}
}

func TestAnthropicStreamCarriesInputTokensFromMessageStart(t *testing.T) {
	// Anthropic emits input_tokens once on message_start and only
	// output_tokens on subsequent message_delta frames. The gateway
	// must remember the input count, otherwise streaming responses
	// expose PromptTokens=0 and route/cost decisions go off the rails.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: message_start\n")
		_, _ = io.WriteString(w, `data: {"type":"message_start","message":{"id":"msg_1","model":"claude","usage":{"input_tokens":17}}}`+"\n\n")
		_, _ = io.WriteString(w, "event: content_block_delta\n")
		_, _ = io.WriteString(w, `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}`+"\n\n")
		_, _ = io.WriteString(w, "event: message_delta\n")
		_, _ = io.WriteString(w, `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`+"\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer srv.Close()

	a, err := New(Options{Name: "anthropic", Endpoint: srv.URL, APIKey: "sk-ant-test"})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := a.Stream(context.Background(), &types.Request{Model: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	var sawUsage bool
	for chunk := range stream {
		if chunk.Usage == nil {
			continue
		}
		sawUsage = true
		if chunk.Usage.PromptTokens != 17 {
			t.Fatalf("prompt tokens not carried from message_start, got %d", chunk.Usage.PromptTokens)
		}
		if chunk.Usage.CompletionTokens != 3 {
			t.Fatalf("completion tokens lost, got %d", chunk.Usage.CompletionTokens)
		}
		if chunk.Usage.TotalTokens != 20 {
			t.Fatalf("total tokens should equal input+output, got %d", chunk.Usage.TotalTokens)
		}
	}
	if !sawUsage {
		t.Fatal("expected at least one chunk with usage populated")
	}
}
