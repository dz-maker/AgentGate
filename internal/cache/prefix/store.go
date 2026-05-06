package prefix

import (
	"time"
)

// Store is the persistence seam under prefix.Service. The default
// implementation is the in-process radix-like map already inside
// service.go. v1.0 introduces this interface so a Redis-backed (or any
// other) store can replace the in-process map without touching the
// extraction / lookup logic.
//
// Why a Store interface and not a generic KV? Two reasons:
//
//   - The radix walk is hash-prefixed per segment, which a flat KV does
//     not capture cleanly. Storing each (tenant, segment-path-hash) →
//     []backendID lets Redis (or any KV) serve our needs without us
//     having to reimplement radix on the wire.
//   - Atomicity of lookup-then-insert is not required: we tolerate
//     racing inserts because the worst case is a few extra entries that
//     the LRU eventually trims.
//
// The interface is small on purpose. Big distributed APIs (compare-and-
// swap, watch, leases) are not necessary here.
type Store interface {
	// LookupCandidates returns candidate backends for the longest prefix
	// match found so far, plus how many segments matched.
	//
	// Implementations should walk the segment list and stop at the first
	// missing segment. matchedTokens accumulates seg.TokenLen for each
	// matched segment.
	LookupCandidates(tenantID string, segments []Segment, halfLife time.Duration) (candidates []CandidateBackend, matchedTokens int)

	// Insert records that backendID processed this segment chain. The
	// store is free to bound capacity (LRU, TTL) however it wants.
	Insert(tenantID string, segments []Segment, backendID string)

	// Pin marks a chain as un-evictable. Used for "this prompt is
	// expensive enough to keep warm at all costs" workflows.
	Pin(tenantID string, segments []Segment)

	// Stats reports any counters the store wants to surface; the keys are
	// store-specific. Always-present keys: "nodes", "evictions",
	// "pinned".
	Stats() map[string]uint64

	// TopK returns the most-hit prefix nodes across all tenants. Used by
	// /admin/prefix/topk and by capacity planning.
	TopK(n int) []TopKey
}
