// Package handler exposes AgentGate's HTTP API.
//
// It normalizes OpenAI-compatible chat completion requests, attaches Agent trace
// metadata, delegates routing to the router package, streams responses back as
// SSE, and records debug/admin state for local inspection.
package handler
