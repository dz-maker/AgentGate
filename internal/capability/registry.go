// Package capability is the runtime registry for per-backend capability
// sheets.
//
// The architecture rationale (see ARCHITECTURE.md §"Capability Registry")
// is that AgentGate sits in front of several inference frameworks with very
// different feature surfaces — vLLM has APC, SGLang has RadixAttention,
// Ollama has neither, OpenAI/Anthropic have neither and also bill per
// token. If the gateway pipeline assumes "all backends are vLLM-shaped" it
// has to be patched in twenty places once the second backend lands.
//
// This package keeps the capability sheet first-class: every adapter
// publishes its own at startup, the registry caches the latest probe
// result, and upper-layer stages (router, cache, fallback, policy) read it
// instead of branching on backend name.
package capability

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

// Sheet is the cached capability snapshot the registry holds for a single
// backend. ProbedAt lets observers tell stale entries apart from fresh ones.
type Sheet struct {
	Backend  string             `json:"backend"`
	Caps     types.Capabilities `json:"capabilities"`
	ProbedAt time.Time          `json:"probed_at"`
	Healthy  bool               `json:"healthy"`
}

// Prober is implemented by adapters that can re-discover their capabilities
// at runtime (e.g. by hitting /v1/models on a vLLM cluster). Adapters that
// have static capabilities just return the same sheet on every call.
type Prober interface {
	Probe(ctx context.Context) (types.Capabilities, error)
}

// Registry stores one capability sheet per backend. Reads are cheap; writes
// happen on the probe interval.
type Registry struct {
	mu      sync.RWMutex
	sheets  map[string]Sheet
	probers map[string]Prober
}

func NewRegistry() *Registry {
	return &Registry{
		sheets:  map[string]Sheet{},
		probers: map[string]Prober{},
	}
}

// Register seeds an initial sheet and (optionally) a prober. If the backend
// has only static capabilities pass nil for prober.
func (r *Registry) Register(name string, caps types.Capabilities, prober Prober) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sheets[name] = Sheet{
		Backend:  name,
		Caps:     caps,
		ProbedAt: time.Now(),
		Healthy:  true,
	}
	if prober != nil {
		r.probers[name] = prober
	}
}

// Update replaces the sheet for a backend without going through the prober.
// Used by health checks that want to mark a backend unhealthy quickly.
func (r *Registry) Update(name string, caps types.Capabilities, healthy bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sheets[name] = Sheet{
		Backend:  name,
		Caps:     caps,
		ProbedAt: time.Now(),
		Healthy:  healthy,
	}
}

// Get returns the cached sheet. The bool is false if the backend was never
// registered. Caller should treat the sheet as immutable.
func (r *Registry) Get(name string) (Sheet, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sheets[name]
	return s, ok
}

// All returns sheets sorted by backend name. Used by /admin endpoints and
// the policy engine when iterating candidates.
func (r *Registry) All() []Sheet {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Sheet, 0, len(r.sheets))
	for _, s := range r.sheets {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Backend < out[j].Backend })
	return out
}

// RefreshAll runs every registered prober once. Errors are reflected in
// Sheet.Healthy; the returned slice is the post-refresh snapshot.
func (r *Registry) RefreshAll(ctx context.Context) []Sheet {
	r.mu.RLock()
	probers := make(map[string]Prober, len(r.probers))
	for name, p := range r.probers {
		probers[name] = p
	}
	r.mu.RUnlock()

	for name, prober := range probers {
		caps, err := prober.Probe(ctx)
		if err != nil {
			r.mu.Lock()
			if existing, ok := r.sheets[name]; ok {
				existing.Healthy = false
				existing.ProbedAt = time.Now()
				r.sheets[name] = existing
			}
			r.mu.Unlock()
			continue
		}
		r.Update(name, caps, true)
	}
	return r.All()
}

// SupportsPrefixSticky returns true iff the backend declares any prefix
// caching mode. The router uses this gate before consulting the prefix
// service.
func (s Sheet) SupportsPrefixSticky() bool {
	if !s.Healthy {
		return false
	}
	switch s.Caps.PrefixCacheMode {
	case types.PrefixCacheAPC, types.PrefixCacheRadix, types.PrefixCacheExternalKV:
		return true
	}
	return s.Caps.SupportsPrefixCache
}
