// Package prefix implements a tenant-isolated prefix locality index used to
// route Agent requests with shared system prompts and tool definitions back to a
// backend instance that recently processed the same prefix.
//
// The index is a radix-like tree keyed by per-segment hashes. It does not store
// KV tensors; it only keeps affinity hints that help backend-side prefix caches
// such as vLLM APC hit more often.
package prefix
