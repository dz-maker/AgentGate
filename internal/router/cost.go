package router

import (
	"sync"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

// CostModel scores backends by expected cost-per-request, blending the
// static CostProfile (USD/1k tokens) with observed latency. The router
// uses this only as a tie-breaker — prefix affinity always wins, because
// a prefix hit dwarfs a few cents of token cost.
//
// Why "expected cost" and not "USD per 1k tokens directly"? Two reasons:
//
//   1. Self-hosted backends report zero cost, but they are not free —
//      they are constrained by GPU minutes. We approximate that by
//      mixing in a latency penalty so a slow vLLM cluster still gets
//      deprioritized vs a cheap, idle one.
//   2. Real per-request cost depends on prompt length. We do not have
//      tokenizer access at routing time, so we use observed mean tokens
//      as a per-(tenant,model) estimate instead.
//
// The model is intentionally simple. A future iteration can replace
// EWMA with a proper Bayesian estimator; the seam is the Score function.
type CostModel struct {
	mu      sync.RWMutex
	stats   map[string]*backendStat // key = backend name
	alpha   float64                 // EWMA smoothing factor in (0,1]
}

type backendStat struct {
	meanTokens   float64
	meanLatency  float64 // ms
	lastUpdated  time.Time
}

func NewCostModel() *CostModel {
	return &CostModel{
		stats: map[string]*backendStat{},
		alpha: 0.2,
	}
}

// Score returns the expected USD-equivalent cost of running this request
// on the given backend. Lower is better. Self-hosted backends with no
// cost profile get a synthesized "GPU-minute" cost from observed latency.
func (cm *CostModel) Score(backendName string, caps types.Capabilities) float64 {
	cm.mu.RLock()
	stat := cm.stats[backendName]
	cm.mu.RUnlock()

	tokens := 1024.0
	latencyMs := 200.0
	if stat != nil {
		if stat.meanTokens > 0 {
			tokens = stat.meanTokens
		}
		if stat.meanLatency > 0 {
			latencyMs = stat.meanLatency
		}
	}

	// Direct USD cost based on token volume (input+output, ratio 70/30
	// matches typical Agent traffic — system prompts dominate the input
	// side; outputs are short tool calls).
	usd := (caps.CostProfile.InputUSDPer1K*0.7 + caps.CostProfile.OutputUSDPer1K*0.3) * tokens / 1000.0

	// For zero-cost-profile self-hosted backends, derive a synthetic cost
	// from latency so the router does not treat them as free relative to
	// each other. $0.0001/ms is a coarse stand-in (≈ $6/min of GPU
	// time — order-of-magnitude correct for an A100 / H100 split slice).
	if usd == 0 {
		usd = latencyMs * 0.0001
	}
	return usd
}

// Observe records a real outcome. Called from the router's Feedback hook.
func (cm *CostModel) Observe(backendName string, tokens int, latency time.Duration) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	stat, ok := cm.stats[backendName]
	if !ok {
		stat = &backendStat{}
		cm.stats[backendName] = stat
	}
	if tokens > 0 {
		if stat.meanTokens == 0 {
			stat.meanTokens = float64(tokens)
		} else {
			stat.meanTokens = ewma(stat.meanTokens, float64(tokens), cm.alpha)
		}
	}
	ms := float64(latency.Milliseconds())
	if ms > 0 {
		if stat.meanLatency == 0 {
			stat.meanLatency = ms
		} else {
			stat.meanLatency = ewma(stat.meanLatency, ms, cm.alpha)
		}
	}
	stat.lastUpdated = time.Now()
}

// Snapshot returns a copy for /admin/cost.
func (cm *CostModel) Snapshot() map[string]CostStat {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	out := make(map[string]CostStat, len(cm.stats))
	for name, s := range cm.stats {
		out[name] = CostStat{
			MeanTokens:  s.meanTokens,
			MeanLatency: s.meanLatency,
			UpdatedAt:   s.lastUpdated,
		}
	}
	return out
}

type CostStat struct {
	MeanTokens  float64   `json:"mean_tokens"`
	MeanLatency float64   `json:"mean_latency_ms"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func ewma(prev, sample, alpha float64) float64 {
	return alpha*sample + (1-alpha)*prev
}
