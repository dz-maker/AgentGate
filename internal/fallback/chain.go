package fallback

import (
	"context"
	"errors"
	"fmt"

	"github.com/agentgate/agentgate/internal/backend"
	"github.com/agentgate/agentgate/pkg/types"
)

// AttemptOutcome is reported on each chain attempt — both successes and
// skips. The caller (handler) translates this into trace span fields and
// logs.
type AttemptOutcome struct {
	BackendName string
	Skipped     bool
	SkipReason  string
	Err         error
}

// Result is what the chain hands back to the caller. Resp is the
// backend's response on success; Outcomes is the audit trail.
type Result struct {
	Resp     *types.Response
	Stream   <-chan types.Chunk
	Backend  backend.Backend
	Outcomes []AttemptOutcome
}

// Registry is what the chain calls into to resolve a backend name to its
// concrete object. Implemented by *backend.Registry plus a small adapter
// in the handler.
type Registry interface {
	ByName(name string) (backend.Backend, bool)
}

// Chain runs an ordered list of backend names, falling back on error or
// open breaker. The first success short-circuits.
//
// stream==true means we want the streaming path; the chain still tries
// each backend in turn but returns the channel from whichever opened it
// first. Once a streaming response is committed (channel returned) we
// cannot fall back further — partial output has already gone to the
// client. This is by design: re-streaming a different model's output
// would surprise the client.
type Chain struct {
	breakers *Set
}

func NewChain(breakers *Set) *Chain {
	return &Chain{breakers: breakers}
}

// Complete walks the chain for non-streaming requests.
func (c *Chain) Complete(ctx context.Context, reg Registry, names []string, req *types.Request) (Result, error) {
	if len(names) == 0 {
		return Result{}, errors.New("fallback chain is empty")
	}
	var outcomes []AttemptOutcome
	var lastErr error
	for _, name := range names {
		b, ok := reg.ByName(name)
		if !ok {
			outcomes = append(outcomes, AttemptOutcome{BackendName: name, Skipped: true, SkipReason: "backend not registered"})
			continue
		}
		breaker := c.breakers.For(name)
		if !breaker.Allow() {
			outcomes = append(outcomes, AttemptOutcome{BackendName: name, Skipped: true, SkipReason: "breaker open"})
			continue
		}

		resp, err := b.Complete(ctx, req)
		if err != nil {
			breaker.Failure()
			outcomes = append(outcomes, AttemptOutcome{BackendName: name, Err: err})
			lastErr = err
			continue
		}
		breaker.Success()
		return Result{Resp: resp, Backend: b, Outcomes: append(outcomes, AttemptOutcome{BackendName: name})}, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("all %d backends in chain were unavailable", len(names))
	}
	return Result{Outcomes: outcomes}, lastErr
}

// Stream walks the chain for streaming requests. See struct doc on why
// once we open a stream we do not fall back further.
func (c *Chain) Stream(ctx context.Context, reg Registry, names []string, req *types.Request) (Result, error) {
	if len(names) == 0 {
		return Result{}, errors.New("fallback chain is empty")
	}
	var outcomes []AttemptOutcome
	var lastErr error
	for _, name := range names {
		b, ok := reg.ByName(name)
		if !ok {
			outcomes = append(outcomes, AttemptOutcome{BackendName: name, Skipped: true, SkipReason: "backend not registered"})
			continue
		}
		breaker := c.breakers.For(name)
		if !breaker.Allow() {
			outcomes = append(outcomes, AttemptOutcome{BackendName: name, Skipped: true, SkipReason: "breaker open"})
			continue
		}
		stream, err := b.Stream(ctx, req)
		if err != nil {
			breaker.Failure()
			outcomes = append(outcomes, AttemptOutcome{BackendName: name, Err: err})
			lastErr = err
			continue
		}
		// Note: we record success on stream OPEN, not on stream completion.
		// The adapter is responsible for its own per-chunk health logic;
		// see vllm/adapter.go inst.success/inst.fail.
		breaker.Success()
		return Result{Stream: stream, Backend: b, Outcomes: append(outcomes, AttemptOutcome{BackendName: name})}, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("all %d backends in chain were unavailable", len(names))
	}
	return Result{Outcomes: outcomes}, lastErr
}
