package ollama

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestOllamaCapabilitiesDeclareNoPrefixCache(t *testing.T) {
	a, err := New(Options{Name: "ollama-edge", Endpoint: "http://localhost:11434"})
	if err != nil {
		t.Fatal(err)
	}
	caps := a.Capabilities()
	if caps.PrefixCacheMode != types.PrefixCacheNone {
		t.Fatalf("ollama must declare PrefixCacheNone, got %q", caps.PrefixCacheMode)
	}
	if caps.SupportsAbort {
		t.Fatal("ollama does not support abort")
	}
	if caps.Vendor != "ollama" {
		t.Fatalf("vendor must be ollama, got %q", caps.Vendor)
	}
}

func TestOllamaCompleteHappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"stream":false`) {
			t.Fatalf("expected stream=false: %s", body)
		}
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{
			Model:           "qwen",
			Message:         ollamaMessage{Role: "assistant", Content: "hi from ollama"},
			Done:            true,
			PromptEvalCount: 5,
			EvalCount:       3,
		})
	}))
	defer srv.Close()

	a, err := New(Options{Name: "ollama-edge", Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := a.Complete(context.Background(), &types.Request{
		Model: "qwen",
		Messages: []types.Message{
			{Role: types.RoleUser, Content: "hello"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Choices[0].Message.ContentString() != "hi from ollama" {
		t.Fatalf("unexpected content: %s", resp.Choices[0].Message.ContentString())
	}
	if resp.Usage == nil || resp.Usage.TotalTokens != 8 {
		t.Fatalf("expected usage total=8, got %+v", resp.Usage)
	}
}

func TestOllamaCompleteTranslatesToolCalls(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{
			Model: "qwen",
			Message: ollamaMessage{
				Role: "assistant",
				ToolCalls: []types.ToolCallDelta{{
					ID:   "call_1",
					Type: "function",
					Function: types.ToolCallFunction{
						Name:      "search",
						Arguments: `{"q":"agentgate"}`,
					},
				}},
			},
			Done: true,
		})
	}))
	defer srv.Close()

	a, err := New(Options{Name: "ollama-edge", Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := a.Complete(context.Background(), &types.Request{Model: "qwen"})
	if err != nil {
		t.Fatal(err)
	}
	choice := resp.Choices[0]
	if choice.FinishReason != "tool_calls" {
		t.Fatalf("expected tool_calls finish, got %q", choice.FinishReason)
	}
	if len(choice.Message.ToolCalls) != 1 || choice.Message.ToolCalls[0].Function.Name != "search" {
		t.Fatalf("tool call not propagated: %#v", choice.Message.ToolCalls)
	}
}

func TestOllamaStreamingTranslatesNDJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `{"model":"qwen","message":{"role":"assistant","content":"he"},"done":false}`)
		fmt.Fprintln(w, `{"model":"qwen","message":{"role":"assistant","content":"llo"},"done":false}`)
		fmt.Fprintln(w, `{"model":"qwen","message":{"role":"assistant","content":""},"done":true,"prompt_eval_count":2,"eval_count":3}`)
	}))
	defer srv.Close()

	a, err := New(Options{Name: "ollama-edge", Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	out, err := a.Stream(context.Background(), &types.Request{
		Model:    "qwen",
		Messages: []types.Message{{Role: types.RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var assembled string
	var seenFinish bool
	for chunk := range out {
		assembled += chunk.Content
		if chunk.FinishReason != "" {
			seenFinish = true
			if chunk.Usage == nil || chunk.Usage.TotalTokens != 5 {
				t.Fatalf("expected usage on done chunk, got %+v", chunk.Usage)
			}
		}
	}
	if assembled != "hello" {
		t.Fatalf("expected 'hello', got %q", assembled)
	}
	if !seenFinish {
		t.Fatal("expected finish chunk")
	}
}
