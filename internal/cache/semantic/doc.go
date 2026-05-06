// Package semantic implements AgentGate's request-level cache.
//
// The cache is layered to match the three different shapes of "this
// request is duplicate" we see in real Agent traffic:
//
//  1. Exact-match: byte-identical request body (popular for /healthz-style
//     synthetic probes and for retry storms after a transient failure).
//
//  2. Tool-result memo: when a deterministic tool was called twice with
//     the same name + arguments inside the same tenant, replay the
//     observed assistant message instead of round-tripping the LLM. This
//     is the highest-value tier in tool-heavy Agent workloads — see
//     BACKGROUND.md §2.2.
//
//  3. Singleflight collapse: if 50 concurrent requests have the same
//     exact-match key, only the first goes upstream; the rest wait and
//     receive the same response. Critical for system-prompt-heavy bursts
//     where a sudden traffic spike would otherwise stampede the backend.
//
// A fourth tier — vector similarity over the user query — is intentionally
// NOT implemented yet. We do not have a good local embedding story (the
// project has zero ML deps by design) and a half-baked embedding+ANN tier
// would either bloat go.mod or produce false positives that silently
// corrupt Agent state. The interface is shaped so a vector tier can be
// dropped in later behind a build tag.
package semantic
