// Package sglang adapts the SGLang inference engine.
//
// SGLang exposes an OpenAI-compatible chat-completions API, so on the wire
// it is mechanically the same as vLLM. The interesting difference is the
// prefix-cache model: SGLang uses RadixAttention rather than vLLM's
// block-hash APC. RadixAttention shares prefixes at any boundary (not just
// 16-token blocks), which means our prefix-locality routing should treat
// SGLang differently — sticky routing is even more valuable there because
// partial-match savings are larger.
//
// We surface that difference through PrefixCacheMode=radix in the
// capability sheet, then reuse the vLLM adapter for transport. The
// gateway pipeline reads the mode (not the vendor name) when deciding
// caching strategy, so SGLang automatically benefits from the right
// behaviour without us forking the HTTP plumbing.
package sglang
