package sglang

import (
	"github.com/agentgate/agentgate/internal/backend/vllm"
	"github.com/agentgate/agentgate/pkg/types"
)

// New builds an SGLang adapter on top of the OpenAI-compatible vLLM
// transport. The only material differences are the capability sheet — we
// advertise PrefixCacheMode=radix and vendor="sglang" — plus the option
// for the caller to override capability-related fields without touching
// transport options.
//
// Pass a vllm.Options; we patch in the SGLang defaults if the caller did
// not set them. This keeps the adapter list short while still letting
// SGLang appear as a distinct backend in /admin/backends.
func New(opts vllm.Options) (*vllm.Adapter, error) {
	if opts.Vendor == "" {
		opts.Vendor = "sglang"
	}
	if opts.PrefixMode == "" {
		opts.PrefixMode = types.PrefixCacheRadix
	}
	if opts.KVProvider == "" {
		opts.KVProvider = "native"
	}
	return vllm.New(opts)
}
