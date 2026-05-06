package openai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestOpenAICloudCapabilitiesDeclareNoPrefixCacheButCarryCost(t *testing.T) {
	a, err := New(Options{
		Name:   "openai-cloud",
		APIKey: "sk-test",
		Cost: types.CostProfile{
			InputUSDPer1K:  0.0025,
			OutputUSDPer1K: 0.01,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	caps := a.Capabilities()
	if caps.PrefixCacheMode != types.PrefixCacheNone {
		t.Fatalf("openai cloud must declare PrefixCacheNone, got %q", caps.PrefixCacheMode)
	}
	if caps.SupportsAbort {
		t.Fatal("openai cloud cannot abort")
	}
	if caps.CostProfile.InputUSDPer1K != 0.0025 {
		t.Fatalf("cost did not propagate: %+v", caps.CostProfile)
	}
}

func TestOpenAICloudCompleteIncludesAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer sk-test" {
			t.Fatalf("missing auth: %q", got)
		}
		fmt.Fprint(w, `{"id":"a","model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`)
	}))
	defer srv.Close()

	a, err := New(Options{Name: "x", Endpoint: srv.URL, APIKey: "sk-test"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := a.Complete(context.Background(), &types.Request{
		Model:    "gpt-4o",
		Messages: []types.Message{{Role: types.RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Choices[0].Message.ContentString() != "hi" {
		t.Fatalf("unexpected content: %s", resp.Choices[0].Message.ContentString())
	}
}
