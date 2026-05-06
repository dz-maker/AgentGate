// Package openai is the cloud OpenAI / OpenAI-compatible-SaaS adapter.
//
// In contrast to the self-hosted OpenAI-compatible adapter (vLLM/SGLang),
// this one is single-endpoint, cannot abort an in-flight request, exposes
// a real CostProfile, and does NOT advertise prefix-caching to the
// router. OpenAI does have its own server-side prompt caching (since
// 2024-10) but we do not control instance affinity, so prefix sticky
// routing has nothing to attach to. We surface that as
// PrefixCacheMode=none so the router skips the sticky path entirely.
//
// The same code path is reused for any OpenAI-protocol cloud (DeepSeek
// API, Moonshot kimi, Together, Fireworks…). Set Vendor and
// Endpoint/APIKey accordingly. The capability sheet is the only thing
// that changes the gateway's behaviour, not the vendor name.
package openai
