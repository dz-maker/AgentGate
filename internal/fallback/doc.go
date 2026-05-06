// Package fallback implements the gateway's resilience layer.
//
// Two related primitives:
//
//   1. Circuit Breaker — per-backend health gate. Opens after N
//      consecutive failures, half-opens after a cooldown to probe, closes
//      again on a successful probe. Standard pattern; we re-implement
//      rather than depend on hystrix-go because the project's go.mod is
//      intentionally tiny and the algorithm is ~80 lines.
//
//   2. Fallback Chain — given an ordered list of backends, try each
//      until one returns a non-error response. The chain participates in
//      the circuit breaker: a backend whose breaker is open is skipped
//      without burning the inflight quota.
//
// Both record their decisions on the Agent Trace span via a small
// callback the API layer plumbs in. That keeps observability honest:
// "the request fell back" is never a silent event in this codebase.
//
// What this package does NOT do:
//   - Retries on the same backend. The breaker treats consecutive
//     failures as evidence; if the first attempt fails, the chain moves
//     on. Retries on a single backend should be implemented at the
//     adapter layer where they can be context-aware (idempotency, etc.).
//   - Hedged requests. Sending the same request to two backends in
//     parallel and racing them is a real strategy but it doubles cost
//     and complicates trace semantics; we intentionally defer it.
package fallback
