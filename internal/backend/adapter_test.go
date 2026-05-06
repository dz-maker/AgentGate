package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/agentgate/agentgate/pkg/types"
)

type registryBackend struct {
	name    string
	healthy bool
	closed  bool
}

func (b *registryBackend) Name() string { return b.name }
func (b *registryBackend) Complete(context.Context, *types.Request) (*types.Response, error) {
	return nil, nil
}
func (b *registryBackend) Stream(context.Context, *types.Request) (<-chan types.Chunk, error) {
	return nil, nil
}
func (b *registryBackend) Capabilities() types.Capabilities { return types.Capabilities{} }
func (b *registryBackend) Healthy() bool                    { return b.healthy }
func (b *registryBackend) Stats() types.BackendStats        { return types.BackendStats{Name: b.name} }
func (b *registryBackend) Close() error {
	b.closed = true
	return nil
}

func TestRegistryDefaultReturnsFirstHealthy(t *testing.T) {
	a := &registryBackend{name: "a"}
	b := &registryBackend{name: "b", healthy: true}
	reg := NewRegistry([]Backend{a, b})

	got, err := reg.Default()
	if err != nil {
		t.Fatal(err)
	}
	if got.Name() != "b" {
		t.Fatalf("got %q, want b", got.Name())
	}
}

func TestRegistryDefaultEmpty(t *testing.T) {
	_, err := NewRegistry(nil).Default()
	if !errors.Is(err, ErrNoHealthyBackend) {
		t.Fatalf("expected ErrNoHealthyBackend, got %v", err)
	}
}

func TestRegistryClose(t *testing.T) {
	a := &registryBackend{name: "a", healthy: true}
	reg := NewRegistry([]Backend{a})
	if err := reg.Close(); err != nil {
		t.Fatal(err)
	}
	if !a.closed {
		t.Fatal("expected backend to close")
	}
}
