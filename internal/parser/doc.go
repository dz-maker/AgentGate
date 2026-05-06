// Package parser contains streaming parsers for Agent-oriented model output.
//
// The current parser incrementally detects tool-call payloads in SSE chunks and
// returns an early-stop signal once a complete call can be safely reconstructed.
// It is deliberately conservative: parse failures fall back to ordinary text
// streaming rather than risking a wrong truncation.
package parser
