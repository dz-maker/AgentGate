package semantic

import (
	"sync"

	"github.com/agentgate/agentgate/pkg/types"
)

// singleflight collapses concurrent backend calls that share a cache key.
// We deliberately re-implement (rather than depending on golang.org/x/sync)
// because go.mod for this project is intentionally minimal — see
// BACKGROUND.md §7 "Why Go". The implementation is straightforward and
// well-tested below.
type singleflight struct {
	mu    sync.Mutex
	calls map[string]*flight
}

type flight struct {
	wg    sync.WaitGroup
	resp  *types.Response
	err   error
	count int
}

func newSingleflight() *singleflight {
	return &singleflight{calls: map[string]*flight{}}
}

// Do runs fn at most once per key concurrently. The bool indicates whether
// this caller is the one that actually executed fn (true) or whether it
// observed the result of another in-flight call (false). Followers receive
// a deep copy of the response so concurrent callers cannot see each other's
// downstream mutations.
func (s *singleflight) Do(key string, fn func() (*types.Response, error)) (*types.Response, error, bool) {
	s.mu.Lock()
	if existing, ok := s.calls[key]; ok {
		existing.count++
		s.mu.Unlock()
		existing.wg.Wait()
		return cloneResponse(existing.resp), existing.err, false
	}
	f := &flight{count: 1}
	f.wg.Add(1)
	s.calls[key] = f
	s.mu.Unlock()

	resp, err := fn()
	f.resp, f.err = resp, err
	f.wg.Done()

	s.mu.Lock()
	delete(s.calls, key)
	s.mu.Unlock()

	return resp, err, true
}

func (s *singleflight) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}
