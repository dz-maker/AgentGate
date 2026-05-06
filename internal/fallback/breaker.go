package fallback

import (
	"sync"
	"time"
)

// State is the breaker's exposed state for /admin/breakers and tests.
type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half_open"
)

// Breaker is a single backend's circuit breaker. Safe for concurrent use.
//
// Thresholds intentionally use small integer counts rather than
// percentages — at the latencies we operate in (single-digit RPS to a few
// hundred RPS per backend), percentage-based breakers can take dozens of
// failures to trip, by which point the user has noticed.
type Breaker struct {
	failureThreshold int
	successThreshold int
	cooldown         time.Duration

	mu              sync.Mutex
	state           State
	consecutiveFail int
	consecutiveOK   int
	openedAt        time.Time
}

type Options struct {
	FailureThreshold int           // failures in a row to trip closed→open
	SuccessThreshold int           // successes in a row to close half-open
	Cooldown         time.Duration // time before open→half_open probe
}

func NewBreaker(opts Options) *Breaker {
	if opts.FailureThreshold <= 0 {
		opts.FailureThreshold = 5
	}
	if opts.SuccessThreshold <= 0 {
		opts.SuccessThreshold = 2
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = 10 * time.Second
	}
	return &Breaker{
		failureThreshold: opts.FailureThreshold,
		successThreshold: opts.SuccessThreshold,
		cooldown:         opts.Cooldown,
		state:            StateClosed,
	}
}

// Allow reports whether the next request can be sent. If the breaker is
// open but the cooldown elapsed, Allow flips it to half-open and lets a
// single probe through.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed, StateHalfOpen:
		return true
	case StateOpen:
		if time.Since(b.openedAt) >= b.cooldown {
			b.state = StateHalfOpen
			b.consecutiveOK = 0
			return true
		}
		return false
	}
	return true
}

// Success records a successful call.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateHalfOpen:
		b.consecutiveOK++
		b.consecutiveFail = 0
		if b.consecutiveOK >= b.successThreshold {
			b.state = StateClosed
			b.consecutiveOK = 0
		}
	default:
		b.consecutiveFail = 0
		b.consecutiveOK++
	}
}

// Failure records a failed call.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFail++
	b.consecutiveOK = 0
	switch b.state {
	case StateClosed:
		if b.consecutiveFail >= b.failureThreshold {
			b.state = StateOpen
			b.openedAt = time.Now()
		}
	case StateHalfOpen:
		// Probe failed: go back to fully open.
		b.state = StateOpen
		b.openedAt = time.Now()
	}
}

// Snapshot returns a non-locking view used by /admin/breakers.
func (b *Breaker) Snapshot() Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Status{
		State:           b.state,
		ConsecutiveFail: b.consecutiveFail,
		ConsecutiveOK:   b.consecutiveOK,
		OpenedAt:        b.openedAt,
	}
}

type Status struct {
	State           State     `json:"state"`
	ConsecutiveFail int       `json:"consecutive_fail"`
	ConsecutiveOK   int       `json:"consecutive_ok"`
	OpenedAt        time.Time `json:"opened_at,omitempty"`
}

// Set is a process-wide bag of breakers, one per backend name.
type Set struct {
	mu     sync.RWMutex
	cohort map[string]*Breaker
	opts   Options
}

func NewSet(opts Options) *Set {
	return &Set{cohort: map[string]*Breaker{}, opts: opts}
}

func (s *Set) For(name string) *Breaker {
	s.mu.RLock()
	if b, ok := s.cohort[name]; ok {
		s.mu.RUnlock()
		return b
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.cohort[name]; ok {
		return b
	}
	b := NewBreaker(s.opts)
	s.cohort[name] = b
	return b
}

func (s *Set) Snapshot() map[string]Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Status, len(s.cohort))
	for name, b := range s.cohort {
		out[name] = b.Snapshot()
	}
	return out
}
