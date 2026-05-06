package prefix

import (
	"encoding/json"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

type SegmentType string

const (
	SegmentClientHint SegmentType = "client_hint"
	SegmentSystem     SegmentType = "system"
	SegmentTools      SegmentType = "tools"
	SegmentHistory    SegmentType = "history"
	SegmentFewShot    SegmentType = "few_shot"
)

type Segment struct {
	Type     SegmentType `json:"type"`
	Hash     uint64      `json:"hash"`
	TokenLen int         `json:"token_len"`
	Content  string      `json:"content,omitempty"`
}

type Match struct {
	BackendID     string             `json:"backend_id,omitempty"`
	MatchedTokens int                `json:"matched_tokens"`
	TotalTokens   int                `json:"total_tokens"`
	MatchedRatio  float64            `json:"matched_ratio"`
	Reason        string             `json:"reason"`
	Candidates    []CandidateBackend `json:"candidates,omitempty"`
}

type CandidateBackend struct {
	BackendID     string    `json:"backend_id"`
	MatchedTokens int       `json:"matched_tokens"`
	HitProb       float64   `json:"hit_prob"`
	Score         float64   `json:"score"`
	LastSeen      time.Time `json:"last_seen"`
}

type Stats struct {
	Tenants       int      `json:"tenants"`
	Nodes         int      `json:"nodes"`
	MaxEntries    int      `json:"max_entries"`
	Lookups       uint64   `json:"lookups"`
	Hits          uint64   `json:"hits"`
	Inserts       uint64   `json:"inserts"`
	Evictions     uint64   `json:"evictions"`
	Pinned        int      `json:"pinned"`
	TopCandidates []TopKey `json:"top_candidates,omitempty"`
}

type TopKey struct {
	TenantID string    `json:"tenant_id"`
	Hash     uint64    `json:"hash"`
	Hits     uint64    `json:"hits"`
	LastHit  time.Time `json:"last_hit"`
	Pinned   bool      `json:"pinned"`
}

type Options struct {
	MaxEntries   int
	HalfLife     time.Duration
	DebugContent bool
	Mirror       Mirror
}

type Service struct {
	mu           sync.RWMutex
	roots        map[string]*node
	nodes        int
	pinnedCount  int
	maxEntries   int
	halfLife     time.Duration
	debugContent bool
	lookups      uint64
	hits         uint64
	inserts      uint64
	evictions    uint64
	mirror       Mirror
}

type node struct {
	hash     uint64
	children map[uint64]*node
	backends map[string]*backendStat
	pinned   bool
	lastHit  time.Time
	hitCount uint64
}

type backendStat struct {
	lastSeen    time.Time
	processedAt time.Time
	hitCount    uint64
}

type nodeRef struct {
	parent  *node
	hash    uint64
	lastHit time.Time
}

func NewService(opts Options) *Service {
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = 100_000
	}
	if opts.HalfLife <= 0 {
		opts.HalfLife = 5 * time.Minute
	}
	return &Service{
		roots:        map[string]*node{},
		maxEntries:   opts.MaxEntries,
		halfLife:     opts.HalfLife,
		debugContent: opts.DebugContent,
		mirror:       opts.Mirror,
	}
}

func (s *Service) Extract(req types.Request) []Segment {
	var segments []Segment
	if req.PrefixHash != "" {
		segments = append(segments, Segment{
			Type:     SegmentClientHint,
			Hash:     hash64(string(SegmentClientHint) + "\x00" + req.PrefixHash),
			TokenLen: 0,
		})
	}

	lastCurrentUser := -1
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == types.RoleUser {
			lastCurrentUser = i
			break
		}
	}

	for i, msg := range req.Messages {
		if i == lastCurrentUser {
			continue
		}
		typ := SegmentHistory
		switch msg.Role {
		case types.RoleSystem:
			typ = SegmentSystem
		case types.RoleAssistant, types.RoleTool:
			typ = SegmentHistory
		case types.RoleUser:
			typ = SegmentHistory
		}
		segments = append(segments, splitContent(typ, msg.ContentString(), s.debugContent)...)
	}

	if len(req.Tools) > 0 {
		raw, _ := json.Marshal(req.Tools)
		segments = append(segments, newSegment(SegmentTools, string(raw), s.debugContent))
	}

	return segments
}

func (s *Service) Lookup(tenantID string, segments []Segment) Match {
	total := tokenTotal(segments)
	if tenantID == "" {
		tenantID = "default"
	}
	if len(segments) == 0 {
		return Match{TotalTokens: total, Reason: "empty_prefix"}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lookups++
	root := s.roots[tenantID]
	if root == nil {
		return Match{TotalTokens: total, Reason: "cold_tenant"}
	}

	cur := root
	matchedTokens := 0
	var candidates []CandidateBackend
	now := time.Now()

	for _, seg := range segments {
		next := cur.children[seg.Hash]
		if next == nil {
			break
		}
		cur = next
		matchedTokens += seg.TokenLen
		cur.lastHit = now
		cur.hitCount++

		for backendID, stat := range cur.backends {
			hitProb := s.hitProb(now, stat.lastSeen)
			candidates = append(candidates, CandidateBackend{
				BackendID:     backendID,
				MatchedTokens: matchedTokens,
				HitProb:       hitProb,
				Score:         float64(matchedTokens) * hitProb,
				LastSeen:      stat.lastSeen,
			})
		}
	}

	if len(candidates) == 0 {
		return Match{MatchedTokens: matchedTokens, TotalTokens: total, MatchedRatio: ratio(matchedTokens, total), Reason: "prefix_miss"}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].LastSeen.After(candidates[j].LastSeen)
		}
		return candidates[i].Score > candidates[j].Score
	})

	s.hits++
	return Match{
		BackendID:     candidates[0].BackendID,
		MatchedTokens: matchedTokens,
		TotalTokens:   total,
		MatchedRatio:  ratio(matchedTokens, total),
		Reason:        "sticky_match",
		Candidates:    dedupeCandidates(candidates),
	}
}

func (s *Service) Insert(tenantID string, segments []Segment, backendID string) {
	if tenantID == "" {
		tenantID = "default"
	}
	if len(segments) == 0 || backendID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	root := s.root(tenantID)
	cur := root
	now := time.Now()

	for _, seg := range segments {
		if cur.children == nil {
			cur.children = map[uint64]*node{}
		}
		next := cur.children[seg.Hash]
		if next == nil {
			next = &node{hash: seg.Hash, children: map[uint64]*node{}, backends: map[string]*backendStat{}}
			cur.children[seg.Hash] = next
			s.nodes++
		}
		cur = next
		cur.lastHit = now
		cur.hitCount++

		stat := cur.backends[backendID]
		if stat == nil {
			stat = &backendStat{processedAt: now}
			cur.backends[backendID] = stat
		}
		stat.lastSeen = now
		stat.hitCount++
	}
	s.inserts++

	if s.nodes > s.maxEntries {
		s.evictOldestLocked()
	}

	if s.mirror != nil {
		s.mirror.MirrorInsert(tenantID, segments, backendID)
	}
}

func (s *Service) Pin(tenantID string, segments []Segment) {
	if tenantID == "" {
		tenantID = "default"
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cur := s.root(tenantID)
	for _, seg := range segments {
		if cur.children == nil {
			cur.children = map[uint64]*node{}
		}
		next := cur.children[seg.Hash]
		if next == nil {
			next = &node{hash: seg.Hash, children: map[uint64]*node{}, backends: map[string]*backendStat{}}
			cur.children[seg.Hash] = next
			s.nodes++
		}
		cur = next
	}
	if !cur.pinned {
		s.pinnedCount++
	}
	cur.pinned = true

	if s.mirror != nil {
		s.mirror.MirrorPin(tenantID, segments)
	}
}

func (s *Service) Stats(topN int) Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := Stats{
		Tenants:    len(s.roots),
		Nodes:      s.nodes,
		MaxEntries: s.maxEntries,
		Lookups:    s.lookups,
		Hits:       s.hits,
		Inserts:    s.inserts,
		Evictions:  s.evictions,
		Pinned:     s.pinnedCount,
	}
	if topN > 0 {
		stats.TopCandidates = s.topLocked(topN)
	}
	return stats
}

func (s *Service) TopK(n int) []TopKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topLocked(n)
}

func (s *Service) root(tenantID string) *node {
	root := s.roots[tenantID]
	if root == nil {
		root = &node{children: map[uint64]*node{}, backends: map[string]*backendStat{}}
		s.roots[tenantID] = root
	}
	return root
}

func (s *Service) hitProb(now, lastSeen time.Time) float64 {
	if lastSeen.IsZero() {
		return 0
	}
	age := now.Sub(lastSeen)
	return math.Exp(-age.Seconds() / s.halfLife.Seconds())
}

func (s *Service) evictOldestLocked() {
	var candidates []nodeRef
	for _, root := range s.roots {
		collectLeaves(root, &candidates)
	}
	if len(candidates) == 0 {
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastHit.Before(candidates[j].lastHit)
	})

	target := s.maxEntries / 10
	if target < 1 {
		target = 1
	}
	removed := 0
	for _, candidate := range candidates {
		if removed >= target || s.nodes <= s.maxEntries {
			break
		}
		child := candidate.parent.children[candidate.hash]
		if child == nil || child.pinned {
			continue
		}
		delete(candidate.parent.children, candidate.hash)
		s.nodes--
		s.evictions++
		removed++
	}
}

func collectLeaves(parent *node, out *[]nodeRef) {
	for hash, child := range parent.children {
		if child.pinned {
			continue
		}
		if len(child.children) == 0 {
			*out = append(*out, nodeRef{parent: parent, hash: hash, lastHit: child.lastHit})
			continue
		}
		collectLeaves(child, out)
	}
}

func (s *Service) topLocked(n int) []TopKey {
	if n <= 0 {
		return nil
	}
	var keys []TopKey
	for tenantID, root := range s.roots {
		walk(root, func(_ string, nd *node) {
			if nd.hash == 0 || nd.hitCount == 0 {
				return
			}
			keys = append(keys, TopKey{
				TenantID: tenantID,
				Hash:     nd.hash,
				Hits:     nd.hitCount,
				LastHit:  nd.lastHit,
				Pinned:   nd.pinned,
			})
		})
	}
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i].Hits == keys[j].Hits {
			return keys[i].LastHit.After(keys[j].LastHit)
		}
		return keys[i].Hits > keys[j].Hits
	})
	if len(keys) > n {
		keys = keys[:n]
	}
	return keys
}

func walk(root *node, fn func(path string, n *node)) {
	var visit func(prefix string, n *node)
	visit = func(prefix string, n *node) {
		fn(prefix, n)
		for hash, child := range n.children {
			visit(prefix+"/"+strconv.FormatUint(hash, 16), child)
		}
	}
	visit("", root)
}

func splitContent(typ SegmentType, content string, debug bool) []Segment {
	const tokensPerSegment = 1024
	const charsPerToken = 4
	const chunkRunes = tokensPerSegment * charsPerToken

	runes := []rune(content)
	if len(runes) <= chunkRunes {
		return []Segment{newSegment(typ, content, debug)}
	}

	var out []Segment
	for start := 0; start < len(runes); start += chunkRunes {
		end := start + chunkRunes
		if end > len(runes) {
			end = len(runes)
		}
		out = append(out, newSegment(typ, string(runes[start:end]), debug))
	}
	return out
}

func newSegment(typ SegmentType, content string, debug bool) Segment {
	normalized := string(typ) + "\x00" + content
	seg := Segment{
		Type:     typ,
		Hash:     hash64(normalized),
		TokenLen: estimateTokens(content),
	}
	if debug {
		seg.Content = content
	}
	return seg
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func estimateTokens(s string) int {
	if s == "" {
		return 0
	}
	tokens := len([]rune(s)) / 4
	if tokens == 0 {
		return 1
	}
	return tokens
}

func tokenTotal(segments []Segment) int {
	total := 0
	for _, seg := range segments {
		total += seg.TokenLen
	}
	return total
}

func ratio(matched, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(matched) / float64(total)
}

func dedupeCandidates(in []CandidateBackend) []CandidateBackend {
	seen := map[string]bool{}
	out := make([]CandidateBackend, 0, len(in))
	for _, c := range in {
		if seen[c.BackendID] {
			continue
		}
		seen[c.BackendID] = true
		out = append(out, c)
	}
	return out
}
