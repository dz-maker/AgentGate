package prefix

import (
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestLookupUsesSharedAgentPrefixAcrossDifferentUserQueries(t *testing.T) {
	svc := NewService(Options{MaxEntries: 100})
	reqA := types.Request{
		TenantID: "tenant-a",
		Messages: []types.Message{
			{Role: types.RoleSystem, Content: "You are a build agent with a strict tool budget."},
			{Role: types.RoleUser, Content: "Inspect service A"},
		},
		Tools: []types.ToolDefinition{{Type: "function", Function: []byte(`{"name":"search"}`)}},
	}
	reqB := reqA
	reqB.Messages = []types.Message{
		{Role: types.RoleSystem, Content: "You are a build agent with a strict tool budget."},
		{Role: types.RoleUser, Content: "Inspect service B"},
	}

	segmentsA := svc.Extract(reqA)
	svc.Insert(reqA.TenantID, segmentsA, "vllm-prod-0")

	match := svc.Lookup(reqB.TenantID, svc.Extract(reqB))
	if match.BackendID != "vllm-prod-0" {
		t.Fatalf("expected sticky backend, got %q", match.BackendID)
	}
	if match.MatchedTokens == 0 {
		t.Fatal("expected shared prefix tokens to match")
	}
	if match.Reason != "sticky_match" {
		t.Fatalf("expected sticky_match, got %q", match.Reason)
	}
}

func TestSplitContentKeepsUTF8Valid(t *testing.T) {
	content := strings.Repeat("中文🙂", 1400)
	segments := splitContent(SegmentSystem, content, true)
	if len(segments) < 2 {
		t.Fatalf("expected content to split, got %d segment", len(segments))
	}
	for i, segment := range segments {
		if !utf8.ValidString(segment.Content) {
			t.Fatalf("segment %d is not valid utf8", i)
		}
	}
}

func TestClientPrefixHashParticipatesInPrefixIndex(t *testing.T) {
	svc := NewService(Options{MaxEntries: 100})
	req := types.Request{
		TenantID:   "tenant-a",
		PrefixHash: "client-hash",
		Messages: []types.Message{
			{Role: types.RoleSystem, Content: "same prompt"},
			{Role: types.RoleUser, Content: "question"},
		},
	}
	segments := svc.Extract(req)
	if len(segments) == 0 || segments[0].Type != SegmentClientHint {
		t.Fatalf("expected client hint segment first, got %#v", segments)
	}
	svc.Insert(req.TenantID, segments, "vllm-prod-0")

	match := svc.Lookup(req.TenantID, svc.Extract(req))
	if match.BackendID != "vllm-prod-0" {
		t.Fatalf("expected client hint match, got %q", match.BackendID)
	}
}

func TestServiceEvictsOldestLeaves(t *testing.T) {
	svc := NewService(Options{MaxEntries: 3})
	req := func(content string) types.Request {
		return types.Request{
			TenantID: "tenant-a",
			Messages: []types.Message{
				{Role: types.RoleSystem, Content: content},
				{Role: types.RoleUser, Content: "q"},
			},
		}
	}
	for _, content := range []string{"old", "warm", "new", "overflow"} {
		r := req(content)
		svc.Insert(r.TenantID, svc.Extract(r), "vllm-prod-0")
	}
	if stats := svc.Stats(0); stats.Evictions == 0 {
		t.Fatalf("expected evictions, got %#v", stats)
	}
}

func TestServiceConcurrency(t *testing.T) {
	svc := NewService(Options{MaxEntries: 10_000})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req := types.Request{
				TenantID: "tenant-a",
				Messages: []types.Message{
					{Role: types.RoleSystem, Content: "shared"},
					{Role: types.RoleUser, Content: id},
				},
			}
			segments := svc.Extract(req)
			svc.Insert(req.TenantID, segments, "vllm-prod-0")
			_ = svc.Lookup(req.TenantID, segments)
		}(i)
	}
	wg.Wait()
}

func BenchmarkPrefixLookup(b *testing.B) {
	svc := NewService(Options{MaxEntries: 100_000})
	req := types.Request{
		TenantID: "tenant-a",
		Messages: []types.Message{
			{Role: types.RoleSystem, Content: strings.Repeat("system prompt ", 200)},
			{Role: types.RoleUser, Content: "question"},
		},
		Tools: []types.ToolDefinition{{Type: "function", Function: []byte(`{"name":"search"}`)}},
	}
	segments := svc.Extract(req)
	svc.Insert(req.TenantID, segments, "vllm-prod-0")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = svc.Lookup(req.TenantID, segments)
	}
}

func BenchmarkSplitContent(b *testing.B) {
	content := strings.Repeat("中文🙂agentgate", 1000)
	for i := 0; i < b.N; i++ {
		_ = splitContent(SegmentSystem, content, false)
	}
}

func TestTenantIsolation(t *testing.T) {
	svc := NewService(Options{MaxEntries: 100})
	req := types.Request{
		TenantID: "tenant-a",
		Messages: []types.Message{
			{Role: types.RoleSystem, Content: "same prompt"},
			{Role: types.RoleUser, Content: "question"},
		},
	}

	svc.Insert(req.TenantID, svc.Extract(req), "vllm-prod-0")
	req.TenantID = "tenant-b"

	match := svc.Lookup(req.TenantID, svc.Extract(req))
	if match.BackendID != "" {
		t.Fatalf("expected isolated tenant miss, got backend %q", match.BackendID)
	}
}
