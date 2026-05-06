package protocol

import (
	"encoding/json"
	"testing"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestChatCompletionRequestNormalize(t *testing.T) {
	raw := json.RawMessage(`{"model":"m","stop":["END","STOP"],"x_agentgate":{"tenant_id":"t","agent_id":"a","trace_id":"tr","step_id":"st","parent_step_id":"p","step_type":"llm_call","prefix_hash":"h"}}`)
	var in ChatCompletionRequest
	if err := json.Unmarshal(raw, &in); err != nil {
		t.Fatal(err)
	}

	req, err := in.Normalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	if req.TenantID != "t" || req.AgentID != "a" || req.TraceID != "tr" || req.StepID != "st" || req.ParentStepID != "p" || req.PrefixHash != "h" {
		t.Fatalf("agentgate options not propagated: %#v", req)
	}
	if len(req.Stop) != 2 || req.Stop[0] != "END" || req.Stop[1] != "STOP" {
		t.Fatalf("unexpected stops: %#v", req.Stop)
	}
	if string(req.Raw) != string(raw) {
		t.Fatal("raw request was not preserved")
	}
}

func TestChatCompletionRequestNormalizeDefaultsTenant(t *testing.T) {
	req, err := (ChatCompletionRequest{Model: "m"}).Normalize(nil)
	if err != nil {
		t.Fatal(err)
	}
	if req.TenantID != "default" {
		t.Fatalf("expected default tenant, got %q", req.TenantID)
	}
}

func TestChatCompletionRequestNormalizeRejectsBadStop(t *testing.T) {
	_, err := (ChatCompletionRequest{Stop: []any{"ok", 42}}).Normalize(nil)
	if err == nil {
		t.Fatal("expected bad stop to fail")
	}
}

func TestChunkFromBackendKeepsContentOnFinishChunk(t *testing.T) {
	resp := ChunkFromBackend(types.Chunk{
		ID:           "c1",
		Model:        "m",
		Content:      "lo",
		FinishReason: "stop",
	})
	if got := resp.Choices[0].Delta.Content; got != "lo" {
		t.Fatalf("final chunk content was dropped: %q", got)
	}
	if got := resp.Choices[0].FinishReason; got != "stop" {
		t.Fatalf("finish reason not propagated: %q", got)
	}
}
