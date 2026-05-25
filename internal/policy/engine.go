package policy

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/agentgate/agentgate/pkg/types"
)

// Document is the on-disk schema. Anything that wants policy changes
// auditable should edit this file rather than touching Go.
type Document struct {
	Routing []RoutingRule `yaml:"routing,omitempty"`
	Cache   []CacheRule   `yaml:"cache,omitempty"`
	Budgets []BudgetRule  `yaml:"budgets,omitempty"`
}

// Match describes the predicate every rule shares: which requests it
// applies to. Empty fields match everything.
type Match struct {
	Tenant   string   `yaml:"tenant,omitempty"`
	Tenants  []string `yaml:"tenants,omitempty"`
	Model    string   `yaml:"model,omitempty"`
	Models   []string `yaml:"models,omitempty"`
	Agent    string   `yaml:"agent,omitempty"`
	Vendor   string   `yaml:"vendor,omitempty"`
	StepType string   `yaml:"step_type,omitempty"`
}

type RoutingRule struct {
	Name    string  `yaml:"name"`
	When    Match   `yaml:"when"`
	Backend string  `yaml:"backend"`
	Fallback []string `yaml:"fallback,omitempty"`
	Weight  float64 `yaml:"weight,omitempty"` // reserved for future weighted routing
}

type CacheRule struct {
	Name    string        `yaml:"name"`
	When    Match         `yaml:"when"`
	Action  string        `yaml:"action"` // "use" | "skip"
	TTL     time.Duration `yaml:"ttl,omitempty"`
	Tier    string        `yaml:"tier,omitempty"` // "exact" | "tool_result" | "all" (default)
}

// BudgetRule is enforced atomically inside Engine.AccountUsage.
type BudgetRule struct {
	Name           string        `yaml:"name"`
	When           Match         `yaml:"when"`
	Window         time.Duration `yaml:"window"`
	MaxTokens      int           `yaml:"max_tokens,omitempty"`
	MaxUSD         float64       `yaml:"max_usd,omitempty"`
	Action         string        `yaml:"action"` // "deny" | "warn"
}

// Decision is the engine's verdict for a single request. All fields are
// optional; the API layer reads only the ones it cares about.
type Decision struct {
	BackendName    string
	BackendChain   []string  // primary first, then fallback chain
	CacheUse       *bool
	CacheTier      string
	CacheTTL       time.Duration
	BudgetExceeded bool
	BudgetReason   string
	BudgetRetryAfter time.Duration
	MatchedRoutingRule string
	MatchedCacheRule   string
	MatchedBudgetRule  string
}

// Engine evaluates rules. Safe for concurrent use; rules are immutable
// once compiled.
type Engine struct {
	doc Document

	mu      sync.Mutex
	usage   map[string]*usageBucket // key=budgetKey()
}

type usageBucket struct {
	windowStart time.Time
	tokens      int
	usd         float64
}

// LoadFromFile reads and parses a YAML policy file. Returns an Engine
// that, when no rules match, behaves as a no-op (matches the gateway's
// pre-policy behaviour).
func LoadFromFile(path string) (*Engine, error) {
	if path == "" {
		return New(Document{}), nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))

	var doc Document
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse policy %s: %w", path, err)
	}
	if err := doc.validate(); err != nil {
		return nil, fmt.Errorf("policy %s: %w", path, err)
	}
	return New(doc), nil
}

func New(doc Document) *Engine {
	return &Engine{
		doc:   doc,
		usage: map[string]*usageBucket{},
	}
}

// Empty reports whether no rules are configured. The handler uses this to
// short-circuit policy evaluation entirely.
func (e *Engine) Empty() bool {
	return len(e.doc.Routing) == 0 && len(e.doc.Cache) == 0 && len(e.doc.Budgets) == 0
}

// Evaluate is called once per incoming request. It does NOT account
// usage — that happens after the response when actual token counts are
// known (see AccountUsage).
func (e *Engine) Evaluate(req types.Request, vendor string) Decision {
	d := Decision{}

	for _, rule := range e.doc.Routing {
		if rule.When.matches(req, vendor) {
			d.BackendName = rule.Backend
			chain := []string{rule.Backend}
			chain = append(chain, rule.Fallback...)
			d.BackendChain = chain
			d.MatchedRoutingRule = rule.Name
			break
		}
	}

	for _, rule := range e.doc.Cache {
		if rule.When.matches(req, vendor) {
			use := rule.Action == "use" || rule.Action == ""
			if rule.Action == "skip" {
				use = false
			}
			d.CacheUse = &use
			d.CacheTier = rule.Tier
			d.CacheTTL = rule.TTL
			d.MatchedCacheRule = rule.Name
			break
		}
	}

	if rule, exceeded, retry := e.checkBudget(req, vendor); exceeded {
		d.BudgetExceeded = true
		d.BudgetReason = fmt.Sprintf("budget %q exceeded", rule)
		d.BudgetRetryAfter = retry
		d.MatchedBudgetRule = rule
	}
	return d
}

// AccountUsage records token / cost consumption against the matching
// budget bucket. Called after every successful response. Cost is computed
// from the backend's CostProfile by the caller — Engine just stores
// numbers.
func (e *Engine) AccountUsage(req types.Request, vendor string, tokens int, usd float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	for _, rule := range e.doc.Budgets {
		if !rule.When.matches(req, vendor) {
			continue
		}
		key := budgetKey(rule.Name, req.TenantID)
		b, ok := e.usage[key]
		if !ok || now.Sub(b.windowStart) > rule.Window {
			b = &usageBucket{windowStart: now}
			e.usage[key] = b
		}
		b.tokens += tokens
		b.usd += usd
	}
}

// SnapshotBudgets returns a copy of every active bucket. Used by
// /admin/policy/budgets.
func (e *Engine) SnapshotBudgets() map[string]BudgetSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]BudgetSnapshot, len(e.usage))
	now := time.Now()
	for key, b := range e.usage {
		// Find the rule for context (window, max).
		// This is O(rules); rules are tiny so it's fine.
		ruleName := strings.SplitN(key, "|", 2)[0]
		var window time.Duration
		var maxTokens int
		var maxUSD float64
		for _, rule := range e.doc.Budgets {
			if rule.Name == ruleName {
				window = rule.Window
				maxTokens = rule.MaxTokens
				maxUSD = rule.MaxUSD
				break
			}
		}
		out[key] = BudgetSnapshot{
			RuleName:     ruleName,
			WindowStart:  b.windowStart,
			WindowEnd:    b.windowStart.Add(window),
			TokensUsed:   b.tokens,
			TokensMax:    maxTokens,
			USDUsed:      b.usd,
			USDMax:       maxUSD,
			ResetIn:      window - now.Sub(b.windowStart),
		}
	}
	return out
}

type BudgetSnapshot struct {
	RuleName    string
	WindowStart time.Time
	WindowEnd   time.Time
	TokensUsed  int
	TokensMax   int
	USDUsed     float64
	USDMax      float64
	ResetIn     time.Duration
}

func (e *Engine) checkBudget(req types.Request, vendor string) (string, bool, time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	for _, rule := range e.doc.Budgets {
		if !rule.When.matches(req, vendor) {
			continue
		}
		key := budgetKey(rule.Name, req.TenantID)
		b, ok := e.usage[key]
		if !ok || now.Sub(b.windowStart) > rule.Window {
			continue
		}
		if rule.MaxTokens > 0 && b.tokens >= rule.MaxTokens {
			if rule.Action == "warn" {
				return "", false, 0
			}
			return rule.Name, true, rule.Window - now.Sub(b.windowStart)
		}
		if rule.MaxUSD > 0 && b.usd >= rule.MaxUSD {
			if rule.Action == "warn" {
				return "", false, 0
			}
			return rule.Name, true, rule.Window - now.Sub(b.windowStart)
		}
	}
	return "", false, 0
}

func budgetKey(rule, tenant string) string {
	return rule + "|" + tenant
}

// matches is the predicate engine. Empty fields match everything; explicit
// fields must match. Lists OR within a field, fields AND across.
func (m Match) matches(req types.Request, vendor string) bool {
	if m.Tenant != "" && m.Tenant != req.TenantID {
		return false
	}
	if len(m.Tenants) > 0 && !slices.Contains(m.Tenants, req.TenantID) {
		return false
	}
	if m.Model != "" && m.Model != req.Model {
		return false
	}
	if len(m.Models) > 0 && !slices.Contains(m.Models, req.Model) {
		return false
	}
	if m.Agent != "" && m.Agent != req.AgentID {
		return false
	}
	if m.StepType != "" && m.StepType != req.StepType {
		return false
	}
	if m.Vendor != "" && m.Vendor != vendor {
		return false
	}
	return true
}

func (d Document) validate() error {
	for _, r := range d.Routing {
		if r.Backend == "" {
			return fmt.Errorf("routing rule %q missing backend", r.Name)
		}
	}
	for _, r := range d.Cache {
		switch r.Action {
		case "", "use", "skip":
		default:
			return fmt.Errorf("cache rule %q has invalid action %q", r.Name, r.Action)
		}
	}
	for _, r := range d.Budgets {
		if r.Window <= 0 {
			return fmt.Errorf("budget rule %q needs a window", r.Name)
		}
		if r.MaxTokens == 0 && r.MaxUSD == 0 {
			return fmt.Errorf("budget rule %q needs max_tokens or max_usd", r.Name)
		}
		switch r.Action {
		case "", "deny", "warn":
		default:
			return fmt.Errorf("budget rule %q has invalid action %q", r.Name, r.Action)
		}
	}
	return nil
}

// ErrEmptyPolicyPath is returned when the caller passes "" but expected a
// real file. The default flow uses LoadFromFile("") to mean "no policy",
// so this is only used by tests.
var ErrEmptyPolicyPath = errors.New("empty policy path")
