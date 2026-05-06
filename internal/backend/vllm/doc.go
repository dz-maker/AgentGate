// Package vllm adapts AgentGate requests to vLLM's OpenAI-compatible HTTP API.
//
// The adapter owns vLLM-specific concerns such as endpoint pools, SSE parsing,
// stop-string injection for tool calls, and prefix-cache-friendly instance
// selection. Those details do not leak into the generic backend interface.
package vllm
