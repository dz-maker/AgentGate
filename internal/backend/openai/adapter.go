package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agentgate/agentgate/internal/backend"
	"github.com/agentgate/agentgate/internal/backend/httpx"
	"github.com/agentgate/agentgate/pkg/types"
)

type Adapter struct {
	name      string
	endpoint  string
	apiKey    string
	headers   map[string]string
	client    *http.Client
	vendor    string
	models    []string
	cost      types.CostProfile
	healthy   atomic.Bool
	inFlight  atomic.Int64
	total     atomic.Uint64
	failed    atomic.Uint64
	mu        sync.RWMutex
	lastErr   string
	lastSeen  time.Time
}

type Options struct {
	Name          string
	Endpoint      string // e.g. https://api.openai.com
	APIKey        string
	Headers       map[string]string
	Vendor        string // openai (default), deepseek, moonshot, together, ...
	Models        []string
	Cost          types.CostProfile
	HeaderTimeout time.Duration
}

func New(opts Options) (*Adapter, error) {
	if opts.Name == "" {
		return nil, errors.New("openai backend name is required")
	}
	if opts.Endpoint == "" {
		opts.Endpoint = "https://api.openai.com"
	}
	vendor := opts.Vendor
	if vendor == "" {
		vendor = "openai"
	}
	a := &Adapter{
		name:     opts.Name,
		endpoint: strings.TrimRight(opts.Endpoint, "/"),
		apiKey:   opts.APIKey,
		headers:  opts.Headers,
		vendor:   vendor,
		models:   append([]string(nil), opts.Models...),
		cost:     opts.Cost,
		client:   httpx.NewClient(httpx.Options{HeaderTimeout: opts.HeaderTimeout}),
		lastSeen: time.Now(),
	}
	a.healthy.Store(true)
	return a, nil
}

func (a *Adapter) Name() string { return a.name }

func (a *Adapter) Capabilities() types.Capabilities {
	return types.Capabilities{
		Vendor:                   a.vendor,
		SupportsStreaming:        true,
		SupportsToolCalling:      true,
		SupportsStructuredOutput: true,
		SupportsLogprobs:         false,
		SupportsAbort:            false,
		// OpenAI does have server-side prompt caching but we cannot route
		// to a specific node, so for the router's purposes there is no
		// affinity to exploit. We mark "none".
		PrefixCacheMode: types.PrefixCacheNone,
		KVProvider:      "none",
		SupportedModels: a.models,
		CostProfile:     a.cost,
	}
}

func (a *Adapter) Healthy() bool { return a.healthy.Load() }

func (a *Adapter) Stats() types.BackendStats {
	a.mu.RLock()
	lastErr, lastSeen := a.lastErr, a.lastSeen
	a.mu.RUnlock()
	return types.BackendStats{
		Name:           a.name,
		Healthy:        a.Healthy(),
		InFlight:       a.inFlight.Load(),
		TotalRequests:  a.total.Load(),
		FailedRequests: a.failed.Load(),
		LastError:      lastErr,
		Instances: []types.InstanceStats{{
			ID:             a.name + "-0",
			Endpoint:       a.endpoint,
			Healthy:        a.Healthy(),
			InFlight:       a.inFlight.Load(),
			TotalRequests:  a.total.Load(),
			FailedRequests: a.failed.Load(),
			LastSeen:       lastSeen,
			LastError:      lastErr,
		}},
	}
}

func (a *Adapter) Close() error {
	if t, ok := a.client.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}

func (a *Adapter) SelectInstance(_ context.Context, _ backend.RoutingHint) (string, error) {
	if !a.healthy.Load() {
		return "", backend.ErrNoHealthyBackend
	}
	return a.name + "-0", nil
}

func (a *Adapter) Complete(ctx context.Context, req *types.Request) (*types.Response, error) {
	a.begin()
	defer a.end()

	body, err := json.Marshal(buildPayload(req, false))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	a.decorate(httpReq)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		a.fail(err)
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("%s status %d: %s", a.vendor, resp.StatusCode, strings.TrimSpace(string(raw)))
		a.fail(err)
		return nil, err
	}
	var out types.Response
	if err := json.Unmarshal(raw, &out); err != nil {
		a.fail(err)
		return nil, err
	}
	if out.Object == "" {
		out.Object = "chat.completion"
	}
	a.success()
	return &out, nil
}

func (a *Adapter) Stream(ctx context.Context, req *types.Request) (<-chan types.Chunk, error) {
	body, err := json.Marshal(buildPayload(req, true))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	a.decorate(httpReq)
	httpReq.Header.Set("Accept", "text/event-stream")

	a.begin()
	resp, err := a.client.Do(httpReq)
	if err != nil {
		a.fail(err)
		a.end()
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		err := fmt.Errorf("%s stream status %d: %s", a.vendor, resp.StatusCode, strings.TrimSpace(string(data)))
		a.fail(err)
		a.end()
		return nil, err
	}

	out := make(chan types.Chunk, 32)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		defer a.end()

		err := readSSE(ctx, resp.Body, func(data []byte) bool {
			chunk, ok := translateChunk(data)
			if !ok {
				return true
			}
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		})
		if err != nil {
			if errors.Is(err, context.Canceled) {
				a.success()
				return
			}
			a.fail(err)
			return
		}
		a.success()
	}()
	return out, nil
}

func buildPayload(req *types.Request, stream bool) map[string]any {
	payload := map[string]any{
		"model":    req.Model,
		"messages": req.Messages,
		"stream":   stream,
	}
	if len(req.Tools) > 0 {
		payload["tools"] = req.Tools
		if req.ToolChoice != nil {
			payload["tool_choice"] = req.ToolChoice
		} else {
			payload["tool_choice"] = "auto"
		}
	}
	if req.Temperature != nil {
		payload["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		payload["top_p"] = *req.TopP
	}
	if req.MaxTokens != nil {
		payload["max_tokens"] = *req.MaxTokens
	}
	if len(req.Stop) > 0 {
		payload["stop"] = req.Stop
	}
	if len(req.ResponseFormat) > 0 {
		payload["response_format"] = json.RawMessage(req.ResponseFormat)
	}
	return payload
}

func (a *Adapter) decorate(r *http.Request) {
	r.Header.Set("Content-Type", "application/json")
	if a.apiKey != "" {
		r.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	for k, v := range a.headers {
		r.Header.Set(k, v)
	}
}

func (a *Adapter) begin() {
	a.inFlight.Add(1)
	a.total.Add(1)
	a.mu.Lock()
	a.lastSeen = time.Now()
	a.mu.Unlock()
}
func (a *Adapter) end() { a.inFlight.Add(-1) }
func (a *Adapter) success() {
	a.mu.Lock()
	a.lastErr = ""
	a.lastSeen = time.Now()
	a.mu.Unlock()
	a.healthy.Store(true)
}
func (a *Adapter) fail(err error) {
	a.failed.Add(1)
	a.mu.Lock()
	if err != nil {
		a.lastErr = err.Error()
	}
	a.lastSeen = time.Now()
	a.mu.Unlock()
}
