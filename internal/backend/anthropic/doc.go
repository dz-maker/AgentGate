// Package anthropic adapts the Anthropic Messages API into AgentGate's
// internal request/response shape.
//
// Unlike OpenAI cloud, Anthropic exposes a different protocol:
//
//   - System prompts live in a top-level "system" field, not as a message
//   - Tool calls are surfaced as content blocks of type "tool_use", not
//     a top-level tool_calls array
//   - Streaming uses event-named SSE (event: content_block_delta) rather
//     than data-only chunks
//   - Anthropic's prompt caching is opt-in via cache_control on individual
//     content blocks; we honor the user's cache_control hint by setting
//     ephemeral on the system + tool definitions when the request asks
//     for share_max
//
// We surface this difference to the gateway through:
//
//   - Vendor="anthropic" in capabilities (so policy DSL can match on it)
//   - PrefixCacheMode="external_kv" only when the user enables Anthropic's
//     prompt cache via cache_control (server-side cache, not in-engine);
//     otherwise PrefixCacheNone, since we cannot route to a specific
//     Anthropic edge node.
package anthropic
