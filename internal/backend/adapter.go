package backend

import (
	"context"
	"errors"

	"github.com/agentgate/agentgate/pkg/types"
)

var ErrNoHealthyBackend = errors.New("no healthy backend instance available")

type RoutingHint struct {
	PreferredInstance string
	PrefixTokens      int
	SessionID         string
	TenantID          string
}

type Backend interface {
	Name() string
	Complete(ctx context.Context, req *types.Request) (*types.Response, error)
	Stream(ctx context.Context, req *types.Request) (<-chan types.Chunk, error)
	Capabilities() types.Capabilities
	Healthy() bool
	Stats() types.BackendStats
}

type InstanceSelector interface {
	SelectInstance(ctx context.Context, hint RoutingHint) (string, error)
}

type Closer interface {
	Close() error
}

type Registry struct {
	backends []Backend
}

func NewRegistry(backends []Backend) *Registry {
	return &Registry{backends: backends}
}

func (r *Registry) Default() (Backend, error) {
	for _, b := range r.backends {
		if b.Healthy() {
			return b, nil
		}
	}
	if len(r.backends) > 0 {
		return r.backends[0], nil
	}
	return nil, ErrNoHealthyBackend
}

func (r *Registry) All() []Backend {
	out := make([]Backend, len(r.backends))
	copy(out, r.backends)
	return out
}

// ByName looks up a backend by its declared name. Used by the policy
// engine and fallback chain to resolve declarative routing decisions
// into concrete backends.
func (r *Registry) ByName(name string) (Backend, bool) {
	for _, b := range r.backends {
		if b.Name() == name {
			return b, true
		}
	}
	return nil, false
}

func (r *Registry) Close() error {
	var firstErr error
	for _, b := range r.backends {
		closer, ok := b.(Closer)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
