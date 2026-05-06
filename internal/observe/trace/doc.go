// Package trace stores the minimal Agent trace model used by AgentGate v0.1.
//
// The store keeps spans in memory for debug endpoints and can append the same
// spans to local JSONL files. This is intentionally small; OTLP or long-term
// storage can be added behind the same boundary later.
package trace
