package fallback

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agentgate/agentgate/internal/backend"
	"github.com/agentgate/agentgate/pkg/types"
)

type fakeBackend struct {
	name      string
	failOnce  bool
	failed    bool
	respText  string
}

func (b *fakeBackend) Name() string { return b.name }
func (b *fakeBackend) Complete(_ context.Context, _ *types.Request) (*types.Response, error) {
	if b.failOnce && !b.failed {
		b.failed = true
		return nil, errors.New("nope")
	}
	return &types.Response{
		Choices: []types.Choice{{Index: 0, Message: types.Message{Role: types.RoleAssistant, Content: b.respText}, FinishReason: "stop"}},
	}, nil
}
func (b *fakeBackend) Stream(context.Context, *types.Request) (<-chan types.Chunk, error) {
	return nil, nil
}
func (b *fakeBackend) Capabilities() types.Capabilities { return types.Capabilities{} }
func (b *fakeBackend) Healthy() bool                    { return true }
func (b *fakeBackend) Stats() types.BackendStats        { return types.BackendStats{Name: b.name} }

type fakeRegistry struct{ items map[string]backend.Backend }

func (r fakeRegistry) ByName(name string) (backend.Backend, bool) {
	b, ok := r.items[name]
	return b, ok
}

func TestChainFallsBackOnError(t *testing.T) {
	primary := &fakeBackend{name: "vllm", failOnce: true}
	secondary := &fakeBackend{name: "ollama", respText: "hi from ollama"}
	reg := fakeRegistry{items: map[string]backend.Backend{
		"vllm": primary, "ollama": secondary,
	}}

	chain := NewChain(NewSet(Options{FailureThreshold: 5, Cooldown: time.Second}))
	res, err := chain.Complete(context.Background(), reg, []string{"vllm", "ollama"}, &types.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backend.Name() != "ollama" {
		t.Fatalf("expected ollama, got %s", res.Backend.Name())
	}
	if len(res.Outcomes) != 2 || res.Outcomes[0].Err == nil || res.Outcomes[1].Err != nil {
		t.Fatalf("unexpected outcomes: %+v", res.Outcomes)
	}
}

func TestChainSkipsOpenBreaker(t *testing.T) {
	primary := &fakeBackend{name: "vllm"}
	secondary := &fakeBackend{name: "ollama", respText: "hi"}
	reg := fakeRegistry{items: map[string]backend.Backend{
		"vllm": primary, "ollama": secondary,
	}}

	breakers := NewSet(Options{FailureThreshold: 1, Cooldown: time.Hour})
	breakers.For("vllm").Failure() // trip primary

	chain := NewChain(breakers)
	res, err := chain.Complete(context.Background(), reg, []string{"vllm", "ollama"}, &types.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backend.Name() != "ollama" {
		t.Fatalf("expected ollama (vllm was open), got %s", res.Backend.Name())
	}
	if !res.Outcomes[0].Skipped || res.Outcomes[0].SkipReason != "breaker open" {
		t.Fatalf("expected breaker-skip outcome: %+v", res.Outcomes[0])
	}
}

func TestChainErrorsWhenAllBackendsFail(t *testing.T) {
	primary := &fakeBackend{name: "vllm", failOnce: true}
	secondary := &fakeBackend{name: "ollama", failOnce: true}
	reg := fakeRegistry{items: map[string]backend.Backend{
		"vllm": primary, "ollama": secondary,
	}}
	chain := NewChain(NewSet(Options{FailureThreshold: 5, Cooldown: time.Second}))
	_, err := chain.Complete(context.Background(), reg, []string{"vllm", "ollama"}, &types.Request{})
	if err == nil {
		t.Fatal("expected error when all backends fail")
	}
}
