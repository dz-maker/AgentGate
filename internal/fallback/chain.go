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
	return c.walk(reg, names, func(b backend.Backend) (Result, error) {
		resp, err := b.Complete(ctx, req)
		if err != nil {
			return Result{}, err
		}
		return Result{Resp: resp}, nil
	})
}

// Stream walks the chain for streaming requests. See struct doc on why
// once we open a stream we do not fall back further.
//
// Note: walk records breaker success on stream OPEN, not on stream
// completion. The adapter is responsible for its own per-chunk health
// logic; see vllm/adapter.go inst.success/inst.fail.
func (c *Chain) Stream(ctx context.Context, reg Registry, names []string, req *types.Request) (Result, error) {
	return c.walk(reg, names, func(b backend.Backend) (Result, error) {
		stream, err := b.Stream(ctx, req)
		if err != nil {
			return Result{}, err
		}
		return Result{Stream: stream}, nil
	})
}

// walk runs the per-name registry lookup + breaker gating loop shared by
// Complete and Stream. attempt returns the request-specific payload
// (Resp or Stream) on success; Backend and Outcomes are filled in here.
func (c *Chain) walk(reg Registry, names []string, attempt func(b backend.Backend) (Result, error)) (Result, error) {
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
		res, err := attempt(b)
		if err != nil {
			breaker.Failure()
			outcomes = append(outcomes, AttemptOutcome{BackendName: name, Err: err})
			lastErr = err
			continue
		}
		breaker.Success()
		res.Backend = b
		res.Outcomes = append(outcomes, AttemptOutcome{BackendName: name})
		return res, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("all %d backends in chain were unavailable", len(names))
	}
	return Result{Outcomes: outcomes}, lastErr
}
