package semantic

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

func makeReq(tenant, model, content string) *types.Request {
	zero := 0.0
	return &types.Request{
		TenantID:    tenant,
		Model:       model,
		Temperature: &zero,
		Messages:    []types.Message{{Role: types.RoleUser, Content: content}},
	}
}

func makeResp(text string) *types.Response {
	return &types.Response{
		Choices: []types.Choice{{Index: 0, Message: types.Message{Role: types.RoleAssistant, Content: text}, FinishReason: "stop"}},
	}
}

func TestExactCacheHitAndTenantIsolation(t *testing.T) {
	svc := New(Options{})
	req := makeReq("tenant-a", "model-x", "hello")

	if hit := svc.Lookup(req); hit.Tier != "" {
		t.Fatal("first lookup should miss")
	}
	svc.Store(req, makeResp("hi"))

	if hit := svc.Lookup(req); hit.Tier != TierExact {
		t.Fatalf("expected exact hit, got %q", hit.Tier)
	}

	other := makeReq("tenant-b", "model-x", "hello")
	if hit := svc.Lookup(other); hit.Tier != "" {
		t.Fatalf("cross-tenant leak: %q", hit.Tier)
	}
}

func TestNoCacheHintBypassesService(t *testing.T) {
	svc := New(Options{})
	req := makeReq("t", "m", "q")
	svc.Store(req, makeResp("a"))

	req2 := makeReq("t", "m", "q")
	req2.CacheControl = types.CachePolicy{PrefixHint: "no_cache"}
	if hit := svc.Lookup(req2); hit.Tier != "" {
		t.Fatalf("no_cache hint should bypass, got %q", hit.Tier)
	}
}

func TestDefaultPolicySkipsNonDeterministicRequests(t *testing.T) {
	svc := New(Options{})
	req := makeReq("t", "m", "q")
	req.Temperature = nil
	svc.Store(req, makeResp("a"))
	if hit := svc.Lookup(req); hit.Tier != "" {
		t.Fatalf("non-deterministic request should not hit cache, got %q", hit.Tier)
	}

	opts := AccessOptions{ExplicitUse: true}
	svc.StoreWithOptions(req, makeResp("forced"), opts)
	if hit := svc.LookupWithOptions(req, opts); hit.Tier != TierExact {
		t.Fatalf("explicit policy use should opt into cache, got %q", hit.Tier)
	}
}

func TestToolResultCacheHitsOnIdenticalToolFollowup(t *testing.T) {
	svc := New(Options{})
	zero := 0.0
	req := &types.Request{
		TenantID:    "t",
		Model:       "m",
		Temperature: &zero,
		Messages: []types.Message{
			{Role: types.RoleUser, Content: "do thing"},
			{Role: types.RoleAssistant, ToolCalls: []types.ToolCallDelta{{ID: "c1"}}},
			{Role: types.RoleTool, ToolCallID: "c1", Content: "{\"weather\":\"sunny\"}"},
		},
	}
	resp := makeResp("now I know")
	svc.Store(req, resp)

	// New request with extra preceding turns but same tool result — should
	// hit the tool tier even though exact key differs.
	req2 := *req
	req2.Messages = append([]types.Message{{Role: types.RoleSystem, Content: "different"}}, req.Messages...)
	hit := svc.Lookup(&req2)
	if hit.Tier != TierTool {
		t.Fatalf("expected tool tier hit, got %q", hit.Tier)
	}
	// Lookup returns a deep clone so concurrent hits cannot mutate each
	// other; identity differs but content must match.
	if hit.Response == nil || len(hit.Response.Choices) != 1 ||
		hit.Response.Choices[0].Message.Content != resp.Choices[0].Message.Content {
		t.Fatalf("expected cloned response with matching content, got %#v", hit.Response)
	}
	if hit.Response == resp {
		t.Fatal("expected lookup to return a clone, not the stored pointer")
	}
}

func TestToolKeySkipsNonZeroTemperature(t *testing.T) {
	temp := 0.7
	req := &types.Request{
		TenantID:    "t",
		Model:       "m",
		Temperature: &temp,
		Messages:    []types.Message{{Role: types.RoleTool, ToolCallID: "c1", Content: "x"}},
	}
	if k := ToolKey(req); k != "" {
		t.Fatalf("non-deterministic temperature must skip tool cache: %q", k)
	}
}

func TestExactCacheRespectsTTL(t *testing.T) {
	svc := New(Options{TTLExact: 10 * time.Millisecond})
	req := makeReq("t", "m", "q")
	svc.Store(req, makeResp("a"))
	time.Sleep(20 * time.Millisecond)
	if hit := svc.Lookup(req); hit.Tier != "" {
		t.Fatalf("expected expired entry, got %q", hit.Tier)
	}
}

func TestSingleflightCollapsesConcurrentCalls(t *testing.T) {
	svc := New(Options{})
	var calls atomic.Int32

	fn := func() (*types.Response, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return makeResp("only-once"), nil
	}

	var wg sync.WaitGroup
	results := make([]*types.Response, 50)
	wasOriginator := make([]bool, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, _, originator := svc.Singleflight().Do("k", fn)
			results[i] = resp
			wasOriginator[i] = originator
		}(i)
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("expected exactly 1 upstream call, got %d", calls.Load())
	}
	originators := 0
	for _, b := range wasOriginator {
		if b {
			originators++
		}
	}
	if originators != 1 {
		t.Fatalf("exactly one caller should be originator, got %d", originators)
	}
}

func TestStatsReflectActivity(t *testing.T) {
	svc := New(Options{})
	req := makeReq("t", "m", "q")

	_ = svc.Lookup(req) // miss
	svc.Store(req, makeResp("a"))
	_ = svc.Lookup(req) // hit

	stats := svc.Stats()
	if stats.HitsExact != 1 || stats.Misses != 1 || stats.Stores != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}
