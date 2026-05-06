package semantic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

// Tier names used in trace spans and /admin/cache/stats.
const (
	TierExact  = "exact"
	TierTool   = "tool_result"
	TierVector = "vector" // reserved for future
)

// Hit is the result of a cache lookup. The caller writes Response straight
// to the client and bypasses the backend.
type Hit struct {
	Tier     string
	Response *types.Response
	StoredAt time.Time
}

// Service is the layered cache. Safe for concurrent use.
type Service struct {
	mu         sync.RWMutex
	exact      map[string]exactEntry
	tool       map[string]toolEntry
	maxEntries int
	ttlExact   time.Duration
	ttlTool    time.Duration

	hitsExact uint64
	hitsTool  uint64
	misses    uint64
	stores    uint64

	flight *singleflight
}

type Options struct {
	MaxEntries int
	TTLExact   time.Duration
	TTLTool    time.Duration
}

// AccessOptions are per-request cache overrides from the policy engine.
// ExplicitUse means an operator deliberately opted this traffic into caching;
// without it, AgentGate only caches deterministic requests (temperature=0).
type AccessOptions struct {
	Skip        bool
	ExplicitUse bool
	Tier        string
	TTL         time.Duration
}

type exactEntry struct {
	resp     *types.Response
	storedAt time.Time
}

type toolEntry struct {
	resp     *types.Response
	storedAt time.Time
}

func New(opts Options) *Service {
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = 10_000
	}
	if opts.TTLExact <= 0 {
		opts.TTLExact = 5 * time.Minute
	}
	if opts.TTLTool <= 0 {
		opts.TTLTool = 10 * time.Minute
	}
	return &Service{
		exact:      map[string]exactEntry{},
		tool:       map[string]toolEntry{},
		maxEntries: opts.MaxEntries,
		ttlExact:   opts.TTLExact,
		ttlTool:    opts.TTLTool,
		flight:     newSingleflight(),
	}
}

// Lookup checks all tiers. Returns the first hit; misses return Hit{}.
//
// When req.CacheControl.PrefixHint == "no_cache" the cache is bypassed —
// callers can ask for fresh results explicitly without touching server
// config.
func (s *Service) Lookup(req *types.Request) Hit {
	return s.LookupWithOptions(req, AccessOptions{})
}

// LookupWithOptions checks all allowed tiers. Returns the first hit; misses
// return Hit{}.
func (s *Service) LookupWithOptions(req *types.Request, opts AccessOptions) Hit {
	if req == nil {
		return Hit{}
	}
	if !s.Cacheable(req, opts) {
		atomic.AddUint64(&s.misses, 1)
		return Hit{}
	}

	now := time.Now()
	exactKey := ExactKey(req)
	ttlExact := s.ttlExact
	ttlTool := s.ttlTool
	if opts.TTL > 0 {
		ttlExact = opts.TTL
		ttlTool = opts.TTL
	}
	tier := normalizeTier(opts.Tier)

	s.mu.RLock()
	if tierAllowsExact(tier) {
		if entry, ok := s.exact[exactKey]; ok && now.Sub(entry.storedAt) < ttlExact {
			storedAt := entry.storedAt
			resp := entry.resp
			s.mu.RUnlock()
			atomic.AddUint64(&s.hitsExact, 1)
			return Hit{Tier: TierExact, Response: cloneResponse(resp), StoredAt: storedAt}
		}
	}
	if tierAllowsTool(tier) {
		if toolKey := ToolKey(req); toolKey != "" {
			if entry, ok := s.tool[toolKey]; ok && now.Sub(entry.storedAt) < ttlTool {
				storedAt := entry.storedAt
				resp := entry.resp
				s.mu.RUnlock()
				atomic.AddUint64(&s.hitsTool, 1)
				return Hit{Tier: TierTool, Response: cloneResponse(resp), StoredAt: storedAt}
			}
		}
	}
	s.mu.RUnlock()

	atomic.AddUint64(&s.misses, 1)
	return Hit{}
}

func (s *Service) Cacheable(req *types.Request, opts AccessOptions) bool {
	if req == nil || opts.Skip {
		return false
	}
	if req.CacheControl.PrefixHint == "no_cache" {
		return false
	}
	if opts.ExplicitUse {
		return true
	}
	return req.Temperature != nil && *req.Temperature == 0
}

func normalizeTier(tier string) string {
	switch tier {
	case "", "all":
		return "all"
	case TierExact, TierTool:
		return tier
	default:
		return "all"
	}
}

func tierAllowsExact(tier string) bool {
	return tier == "all" || tier == TierExact
}

func tierAllowsTool(tier string) bool {
	return tier == "all" || tier == TierTool
}

// Store inserts a fresh response into all applicable tiers.
//
// Streaming responses are not cached: tier semantics of "replay the same
// chunks" are subtle (do we replay timing? early-stop position?) and
// caching them would surprise callers. We let the caller decide whether to
// invoke Store for non-streaming responses only.
func (s *Service) Store(req *types.Request, resp *types.Response) {
	s.StoreWithOptions(req, resp, AccessOptions{})
}

// StoreWithOptions inserts a fresh response into the allowed tiers.
func (s *Service) StoreWithOptions(req *types.Request, resp *types.Response, opts AccessOptions) {
	if req == nil || resp == nil {
		return
	}
	if !s.Cacheable(req, opts) {
		return
	}
	now := time.Now()
	exactKey := ExactKey(req)
	toolKey := ToolKey(req)
	tier := normalizeTier(opts.Tier)

	s.mu.Lock()
	defer s.mu.Unlock()

	if tierAllowsExact(tier) {
		if len(s.exact) >= s.maxEntries {
			s.evictOneExactLocked()
		}
		s.exact[exactKey] = exactEntry{resp: resp, storedAt: now}
	}

	if tierAllowsTool(tier) && toolKey != "" && resp.Choices != nil {
		if len(s.tool) >= s.maxEntries {
			s.evictOneToolLocked()
		}
		s.tool[toolKey] = toolEntry{resp: resp, storedAt: now}
	}
	atomic.AddUint64(&s.stores, 1)
}

// Singleflight returns a guard that ensures at most one concurrent compute
// per exact key. Caller passes a fetch fn; concurrent callers with the
// same key share its result.
func (s *Service) Singleflight() *singleflight { return s.flight }

func (s *Service) evictOneExactLocked() {
	// Drop the oldest entry. O(n) is fine: maxEntries is bounded and this
	// happens only when full.
	var oldestKey string
	var oldest time.Time
	for k, v := range s.exact {
		if oldestKey == "" || v.storedAt.Before(oldest) {
			oldestKey = k
			oldest = v.storedAt
		}
	}
	delete(s.exact, oldestKey)
}

func (s *Service) evictOneToolLocked() {
	var oldestKey string
	var oldest time.Time
	for k, v := range s.tool {
		if oldestKey == "" || v.storedAt.Before(oldest) {
			oldestKey = k
			oldest = v.storedAt
		}
	}
	delete(s.tool, oldestKey)
}

type Stats struct {
	ExactEntries int    `json:"exact_entries"`
	ToolEntries  int    `json:"tool_entries"`
	HitsExact    uint64 `json:"hits_exact"`
	HitsTool     uint64 `json:"hits_tool"`
	Misses       uint64 `json:"misses"`
	Stores       uint64 `json:"stores"`
	InFlight     int    `json:"singleflight_in_flight"`
}

func (s *Service) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Stats{
		ExactEntries: len(s.exact),
		ToolEntries:  len(s.tool),
		HitsExact:    atomic.LoadUint64(&s.hitsExact),
		HitsTool:     atomic.LoadUint64(&s.hitsTool),
		Misses:       atomic.LoadUint64(&s.misses),
		Stores:       atomic.LoadUint64(&s.stores),
		InFlight:     s.flight.size(),
	}
}

// cloneResponse deep-copies a cached response so concurrent hits cannot
// see each other's mutations. The marshal-unmarshal round-trip is safe
// for types.Response and avoids hand-maintaining a clone for every nested
// field. Called on every cache hit; cost is one alloc per hit, which is
// dominated by the network call we just saved.
func cloneResponse(in *types.Response) *types.Response {
	if in == nil {
		return nil
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return in
	}
	var out types.Response
	if err := json.Unmarshal(raw, &out); err != nil {
		return in
	}
	return &out
}

// ExactKey hashes the parts of a request that determine response equality.
// Tenant scoping prevents cross-tenant leakage even on identical prompts.
func ExactKey(req *types.Request) string {
	h := sha256.New()
	stops := append([]string(nil), req.Stop...)
	sort.Strings(stops)
	enc := json.NewEncoder(writerFn(func(b []byte) (int, error) { return h.Write(b) }))
	_ = enc.Encode(keyShape{
		Tenant:      req.TenantID,
		Backend:     req.RoutedBackend,
		Model:       req.Model,
		Messages:    req.Messages,
		Tools:       req.Tools,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		MaxTokens:   req.MaxTokens,
		Stop:        stops,
		ResponseFmt: req.ResponseFormat,
	})
	return hex.EncodeToString(h.Sum(nil))
}

type keyShape struct {
	Tenant      string                 `json:"t"`
	Backend     string                 `json:"b,omitempty"`
	Model       string                 `json:"m"`
	Messages    []types.Message        `json:"msg"`
	Tools       []types.ToolDefinition `json:"tools,omitempty"`
	Temperature *float64               `json:"temp,omitempty"`
	TopP        *float64               `json:"tp,omitempty"`
	MaxTokens   *int                   `json:"mt,omitempty"`
	Stop        []string               `json:"stop,omitempty"`
	ResponseFmt json.RawMessage        `json:"rf,omitempty"`
}

type writerFn func([]byte) (int, error)

func (f writerFn) Write(p []byte) (int, error) { return f(p) }

// ToolKey returns a cache key when the last assistant turn was a single
// deterministic tool result. We cache the LLM's response to "given this
// tool result, what's the next reasoning step" because Agent runs
// frequently re-invoke the same tool with the same args.
//
// Returns "" when tool-result caching does not apply (cache miss path).
func ToolKey(req *types.Request) string {
	if req == nil || len(req.Messages) == 0 {
		return ""
	}
	if req.Temperature != nil && *req.Temperature > 0 {
		// Non-deterministic generation: caching would be misleading.
		return ""
	}
	last := req.Messages[len(req.Messages)-1]
	if last.Role != types.RoleTool {
		return ""
	}

	h := sha256.New()
	enc := json.NewEncoder(writerFn(func(b []byte) (int, error) { return h.Write(b) }))
	type shape struct {
		Tenant     string `json:"t"`
		Model      string `json:"m"`
		ToolCallID string `json:"id"`
		Content    string `json:"c"`
	}
	_ = enc.Encode(shape{
		Tenant:     req.TenantID,
		Model:      req.Model,
		ToolCallID: last.ToolCallID,
		Content:    last.ContentString(),
	})
	return "tool:" + hex.EncodeToString(h.Sum(nil))
}
