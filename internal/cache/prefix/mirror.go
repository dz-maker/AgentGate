package prefix

import (
	"sync"
	"time"
)

// Mirror is the seam for distributing prefix-affinity state across multiple
// AgentGate instances. The local in-process radix map is always
// authoritative for the hot path (lookups must stay sub-millisecond);
// writes are additionally fan-out'ed to a Mirror so other gateway
// instances learn about them within seconds.
//
// This shape — local-first with eventually-consistent broadcast — matches
// the failure semantics we want:
//
//   - if the mirror is down, the gateway still serves traffic; it just
//     loses cross-instance affinity until the mirror recovers.
//   - if a request lands on a different instance than the one that warmed
//     a prefix, the worst case is one missed sticky-route, not a 5xx.
//
// We deliberately do not expose Lookup on the Mirror: in production our
// observation is that asymmetric latency to a remote Redis is worse than
// a few stale prefix entries. If a deployment really needs distributed
// lookup, MirrorWithLookup (an interface composition) can layer on it.
//
// The Redis-backed implementation is intentionally not in this package
// to keep go.mod minimal. It lives as a contributed adapter under
// docs/recipes/redis-mirror.md for users who need it.
type Mirror interface {
	// MirrorInsert is called after the local Service successfully records
	// a prefix-segment chain. Implementations should be non-blocking
	// (queue + background flush) so the request path stays fast.
	MirrorInsert(tenantID string, segments []Segment, backendID string)

	// MirrorPin is called when a prefix is pinned.
	MirrorPin(tenantID string, segments []Segment)

	// Stats reports counters: pending, sent, dropped, errors.
	Stats() MirrorStats
}

type MirrorStats struct {
	Pending uint64 `json:"pending"`
	Sent    uint64 `json:"sent"`
	Dropped uint64 `json:"dropped"`
	Errors  uint64 `json:"errors"`
}

// LocalMirror is a Mirror that stays in-process. It is useful for tests
// and for single-instance deployments that want the same code path
// regardless of whether a remote mirror is configured.
type LocalMirror struct {
	mu         sync.Mutex
	inserts    []MirrorEvent
	pins       []MirrorEvent
	maxBuffer  int
	statsSent  uint64
	statsDrop  uint64
}

type MirrorEvent struct {
	TenantID  string
	Segments  []Segment
	BackendID string
	At        time.Time
}

func NewLocalMirror(bufferSize int) *LocalMirror {
	if bufferSize <= 0 {
		bufferSize = 1024
	}
	return &LocalMirror{maxBuffer: bufferSize}
}

func (m *LocalMirror) MirrorInsert(tenantID string, segments []Segment, backendID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.inserts) >= m.maxBuffer {
		// Drop oldest. In production this would back-pressure the queue
		// or page an operator, but for an in-process mirror we just want
		// to bound memory.
		m.inserts = m.inserts[1:]
		m.statsDrop++
	}
	segCopy := append([]Segment(nil), segments...)
	m.inserts = append(m.inserts, MirrorEvent{
		TenantID:  tenantID,
		Segments:  segCopy,
		BackendID: backendID,
		At:        time.Now(),
	})
	m.statsSent++
}

func (m *LocalMirror) MirrorPin(tenantID string, segments []Segment) {
	m.mu.Lock()
	defer m.mu.Unlock()
	segCopy := append([]Segment(nil), segments...)
	m.pins = append(m.pins, MirrorEvent{
		TenantID: tenantID,
		Segments: segCopy,
		At:       time.Now(),
	})
}

func (m *LocalMirror) Stats() MirrorStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return MirrorStats{
		Pending: uint64(len(m.inserts)),
		Sent:    m.statsSent,
		Dropped: m.statsDrop,
	}
}

// Drain consumes all buffered insert events. Test helper.
func (m *LocalMirror) Drain() []MirrorEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.inserts
	m.inserts = nil
	return out
}
