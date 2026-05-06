package policy

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestRoutingFirstMatchWins(t *testing.T) {
	eng := New(Document{Routing: []RoutingRule{
		{Name: "premium-to-vllm", When: Match{Tenant: "premium"}, Backend: "vllm-prod"},
		{Name: "default", Backend: "ollama-edge"},
	}})

	req := types.Request{TenantID: "premium", Model: "qwen"}
	d := eng.Evaluate(req, "vllm")
	if d.BackendName != "vllm-prod" {
		t.Fatalf("expected vllm-prod, got %q", d.BackendName)
	}
	if d.MatchedRoutingRule != "premium-to-vllm" {
		t.Fatalf("rule name not propagated")
	}

	req2 := types.Request{TenantID: "free", Model: "qwen"}
	d2 := eng.Evaluate(req2, "ollama")
	if d2.BackendName != "ollama-edge" {
		t.Fatalf("default rule did not match: %q", d2.BackendName)
	}
}

func TestRoutingFallbackChainPropagates(t *testing.T) {
	eng := New(Document{Routing: []RoutingRule{
		{Name: "claude", When: Match{Vendor: "anthropic"}, Backend: "anthropic-cloud", Fallback: []string{"openai-cloud", "vllm-prod"}},
	}})
	d := eng.Evaluate(types.Request{TenantID: "t"}, "anthropic")
	if len(d.BackendChain) != 3 || d.BackendChain[2] != "vllm-prod" {
		t.Fatalf("fallback chain not propagated: %v", d.BackendChain)
	}
}

func TestCacheRuleSkipDisablesSemanticCache(t *testing.T) {
	eng := New(Document{Cache: []CacheRule{
		{Name: "no-cache-for-tools", When: Match{StepType: "tool_call"}, Action: "skip"},
	}})
	d := eng.Evaluate(types.Request{StepType: "tool_call"}, "vllm")
	if d.CacheUse == nil || *d.CacheUse {
		t.Fatalf("cache should be disabled for tool_call: %+v", d.CacheUse)
	}
}

func TestBudgetExceededDeniesRequest(t *testing.T) {
	eng := New(Document{Budgets: []BudgetRule{
		{Name: "anthropic-daily", Window: 24 * time.Hour, MaxUSD: 1.0, Action: "deny", When: Match{Vendor: "anthropic"}},
	}})
	req := types.Request{TenantID: "t"}
	eng.AccountUsage(req, "anthropic", 1000, 0.99)
	if d := eng.Evaluate(req, "anthropic"); d.BudgetExceeded {
		t.Fatal("should not be exceeded yet")
	}
	eng.AccountUsage(req, "anthropic", 1000, 0.5)
	d := eng.Evaluate(req, "anthropic")
	if !d.BudgetExceeded {
		t.Fatal("should be exceeded")
	}
	if d.BudgetReason == "" {
		t.Fatal("expected budget reason")
	}
}

func TestBudgetWindowResets(t *testing.T) {
	eng := New(Document{Budgets: []BudgetRule{
		{Name: "tight", Window: 50 * time.Millisecond, MaxTokens: 100, Action: "deny"},
	}})
	req := types.Request{TenantID: "t"}
	eng.AccountUsage(req, "vllm", 200, 0)
	if d := eng.Evaluate(req, "vllm"); !d.BudgetExceeded {
		t.Fatal("should be exceeded")
	}
	time.Sleep(80 * time.Millisecond)
	if d := eng.Evaluate(req, "vllm"); d.BudgetExceeded {
		t.Fatal("window should have reset")
	}
}

func TestBudgetWarnDoesNotDeny(t *testing.T) {
	eng := New(Document{Budgets: []BudgetRule{
		{Name: "soft", Window: time.Hour, MaxTokens: 1, Action: "warn"},
	}})
	req := types.Request{TenantID: "t"}
	eng.AccountUsage(req, "vllm", 1000, 0)
	if d := eng.Evaluate(req, "vllm"); d.BudgetExceeded {
		t.Fatal("warn budgets must not block requests")
	}
}

func TestLoadFromFileParsesAndEvaluates(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "policy.yaml")
	yaml := []byte(`
routing:
  - name: free-to-ollama
    when:
      tenant: free
    backend: ollama-edge
  - name: default
    backend: vllm-prod
cache:
  - name: never-cache-secrets
    when:
      tenant: secrets
    action: skip
budgets:
  - name: budget-cheap
    when:
      vendor: anthropic
    window: 1h
    max_tokens: 1000
    action: deny
`)
	if err := writeFile(path, yaml); err != nil {
		t.Fatal(err)
	}
	eng, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if eng.Empty() {
		t.Fatal("policy should be non-empty")
	}
	d := eng.Evaluate(types.Request{TenantID: "free"}, "ollama")
	if d.BackendName != "ollama-edge" {
		t.Fatalf("expected ollama-edge, got %q", d.BackendName)
	}
}

func writeFile(path string, body []byte) error {
	return osWriteFile(path, body, 0o644)
}
