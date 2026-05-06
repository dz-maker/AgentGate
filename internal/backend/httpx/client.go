// Package httpx is a small helper used by every HTTP-based backend adapter.
//
// vLLM, SGLang, Ollama, OpenAI and Anthropic all need essentially the same
// transport tuning — high idle-conn ceilings, long response-header timeout
// for cold prefills, no overall WriteTimeout because SSE streams stay open.
// Pulling that into one place keeps the per-adapter file focused on the
// protocol-specific quirks (payload shaping, capability advertisement).
package httpx

import (
	"net"
	"net/http"
	"time"
)

type Options struct {
	HeaderTimeout time.Duration
	DialTimeout   time.Duration
	IdlePerHost   int
	MaxIdle       int
}

// NewClient builds an HTTP client tuned for streaming inference backends.
// Caller can mutate the returned client (e.g. to inject a wrapping
// transport for tracing) — every adapter owns its own instance.
func NewClient(opts Options) *http.Client {
	if opts.HeaderTimeout <= 0 {
		opts.HeaderTimeout = 30 * time.Second
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 5 * time.Second
	}
	if opts.IdlePerHost <= 0 {
		opts.IdlePerHost = 128
	}
	if opts.MaxIdle <= 0 {
		opts.MaxIdle = 512
	}

	transport := &http.Transport{
		MaxIdleConns:          opts.MaxIdle,
		MaxIdleConnsPerHost:   opts.IdlePerHost,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: opts.HeaderTimeout,
		ForceAttemptHTTP2:     true,
		DialContext: (&net.Dialer{
			Timeout:   opts.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	return &http.Client{Transport: transport}
}
