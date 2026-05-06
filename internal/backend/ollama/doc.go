// Package ollama is the shallow Ollama backend adapter.
//
// It is deliberately shallow — Ollama is single-instance, has no APC, no
// abort, no structured-output guarantee — and that is exactly the point.
// Building this second adapter forced AgentGate's Backend interface and
// Capability sheet to stop assuming the world is vLLM-shaped.
//
// Concretely the adapter:
//   - declares PrefixCacheNone so the router skips sticky logic
//   - speaks Ollama's /api/chat protocol (similar to OpenAI but uses NDJSON
//     instead of SSE for streaming)
//   - silently ignores unsupported request fields (logprobs,
//     response_format) rather than returning errors, since Ollama clients
//     in the wild expect lenient handling
//
// What this adapter is NOT: a production-grade Ollama client. We don't
// surface model loading state, the /api/show metadata, or model digests.
// That belongs in v1.x once a real edge-deployment user shows up.
package ollama
