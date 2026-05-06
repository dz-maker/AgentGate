// Package policy is AgentGate's declarative control plane.
//
// In v0.1 routing was hard-coded ("use the first healthy backend, prefer
// instances with prefix affinity"). That is fine when there is one
// backend but breaks down quickly when an operator wants:
//
//   - "tenant=premium goes to the GPU pool, others go to Ollama"
//   - "claude requests cost <= $50 / day per tenant; over budget → 429"
//   - "if vLLM cluster is degraded, fall back to OpenAI cloud but only
//      for the chat model, not the tool model"
//
// Encoding all those into Go would calcify the gateway. Instead the
// policy engine reads a YAML file (or a hot-reloadable HTTP endpoint
// later) and evaluates per-request. The evaluator is intentionally
// declarative — no scripting, no Turing-completeness — to keep the
// configuration auditable and the failure modes obvious.
//
// Three rule classes:
//
//   - Routing rules: pick a backend by matching tenant / model / agent.
//     First match wins; absence of any matching rule falls through to the
//     legacy router (which is itself the safest default).
//
//   - Cache rules: enable / disable / TTL-override the semantic cache for
//     a request. Useful for "never cache for tenant=acme/sales" cases.
//
//   - Budget rules: token / cost ceilings per tenant per window. When
//     exceeded the engine flags the request and the API layer maps that
//     to 429 Too Many Requests with a Retry-After hint.
//
// Why not a full DSL like OPA or CEL? Both work, but each adds a heavy
// dep. AgentGate's policy surface is small enough that hand-rolled
// matchers are clearer than a generic expression evaluator, and we can
// always swap to CEL later by re-implementing the Engine interface.
package policy
