package router

import (
	"context"
	"testing"

	"github.com/agentgate/agentgate/internal/backend"
	"github.com/agentgate/agentgate/internal/cache/prefix"
	"github.com/agentgate/agentgate/pkg/types"
)

type routeBackend struct {
	name     string
	caps     types.Capabilities
	selected backend.RoutingHint
}

func (b *routeBackend) Name() string { return b.name }
func (b *routeBackend) Complete(context.Context, *types.Request) (*types.Response, error) {
	return nil, nil
}
func (b *routeBackend) Stream(context.Context, *types.Request) (<-chan types.Chunk, error) {
	return nil, nil
}
func (b *routeBackend) Capabilities() types.Capabilities { return b.caps }
func (b *routeBackend) Healthy() bool                    { return true }
func (b *routeBackend) Stats() types.BackendStats        { return types.BackendStats{Name: b.name} }
func (b *routeBackend) SelectInstance(ctx context.Context, hint backend.RoutingHint) (string, error) {
	b.selected = hint
	if hint.PreferredInstance != "" {
		return hint.PreferredInstance, nil
	}
	return b.name + "-0", nil
}

func TestRouteUsesPrefixOnlyWhenBackendSupportsIt(t *testing.T) {
	b := &routeBackend{name: "plain"}
	reg := backend.NewRegistry([]backend.Backend{b})
	svc := prefix.NewService(prefix.Options{MaxEntries: 100})
	r := New(reg, svc)

	decision, err := r.Route(context.Background(), types.Request{
		TenantID: "tenant-a",
		Messages: []types.Message{
			{Role: types.RoleSystem, Content: "shared"},
			{Role: types.RoleUser, Content: "q"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.PrefixMatch.Reason != "prefix_unsupported" {
		t.Fatalf("expected prefix_unsupported, got %q", decision.PrefixMatch.Reason)
	}
	if b.selected.PreferredInstance != "" || b.selected.PrefixTokens != 0 {
		t.Fatalf("prefix hint leaked to unsupported backend: %#v", b.selected)
	}
}

func TestRoutePassesStickyHintToInstanceSelector(t *testing.T) {
	b := &routeBackend{name: "vllm", caps: types.Capabilities{SupportsPrefixCache: true}}
	reg := backend.NewRegistry([]backend.Backend{b})
	svc := prefix.NewService(prefix.Options{MaxEntries: 100})
	req := types.Request{
		TenantID: "tenant-a",
		Messages: []types.Message{
			{Role: types.RoleSystem, Content: "shared"},
			{Role: types.RoleUser, Content: "q"},
		},
	}
	segments := svc.Extract(req)
	svc.Insert(req.TenantID, segments, "vllm-0")

	decision, err := New(reg, svc).Route(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if decision.InstanceID != "vllm-0" {
		t.Fatalf("expected sticky instance, got %q", decision.InstanceID)
	}
	if b.selected.PreferredInstance != "vllm-0" {
		t.Fatalf("expected sticky hint, got %#v", b.selected)
	}
}
