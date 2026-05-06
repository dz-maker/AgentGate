package anthropic

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

const defaultEndpoint = "https://api.anthropic.com"
const defaultAnthropicVersion = "2023-06-01"

type Adapter struct {
	name     string
	endpoint string
	apiKey   string
	version  string
	models   []string
	cost     types.CostProfile
	headers  map[string]string
	client   *http.Client

	healthy  atomic.Bool
	inFlight atomic.Int64
	total    atomic.Uint64
	failed   atomic.Uint64
	mu       sync.RWMutex
	lastErr  string
	lastSeen time.Time
}

type Options struct {
	Name             string
	Endpoint         string
	APIKey           string
	AnthropicVersion string
	Models           []string
	Cost             types.CostProfile
	Headers          map[string]string
	HeaderTimeout    time.Duration
}

func New(opts Options) (*Adapter, error) {
	if opts.Name == "" {
		return nil, errors.New("anthropic backend name is required")
	}
	if opts.APIKey == "" {
		return nil, errors.New("anthropic backend api key is required")
	}
	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	version := opts.AnthropicVersion
	if version == "" {
		version = defaultAnthropicVersion
	}
	a := &Adapter{
		name:     opts.Name,
		endpoint: strings.TrimRight(endpoint, "/"),
		apiKey:   opts.APIKey,
		version:  version,
		models:   append([]string(nil), opts.Models...),
		cost:     opts.Cost,
		headers:  opts.Headers,
		client:   httpx.NewClient(httpx.Options{HeaderTimeout: opts.HeaderTimeout}),
		lastSeen: time.Now(),
	}
	a.healthy.Store(true)
	return a, nil
}

func (a *Adapter) Name() string { return a.name }

func (a *Adapter) Capabilities() types.Capabilities {
	return types.Capabilities{
		Vendor:                   "anthropic",
		SupportsStreaming:        true,
		SupportsToolCalling:      true,
		SupportsStructuredOutput: false,
		SupportsAbort:            false,
		PrefixCacheMode:          types.PrefixCacheNone,
		KVProvider:               "none",
		SupportedModels:          a.models,
		CostProfile:              a.cost,
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
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint+"/v1/messages", bytes.NewReader(body))
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
		err := fmt.Errorf("anthropic status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		a.fail(err)
		return nil, err
	}
	var msg messagesResponse
	if err := json.Unmarshal(raw, &msg); err != nil {
		a.fail(err)
		return nil, err
	}
	a.success()
	return msg.toResponse(req.Model), nil
}

func (a *Adapter) Stream(ctx context.Context, req *types.Request) (<-chan types.Chunk, error) {
	body, err := json.Marshal(buildPayload(req, true))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint+"/v1/messages", bytes.NewReader(body))
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
		err := fmt.Errorf("anthropic stream status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		a.fail(err)
		a.end()
		return nil, err
	}

	out := make(chan types.Chunk, 32)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		defer a.end()

		err := readMessagesSSE(ctx, resp.Body, req.Model, func(c types.Chunk) bool {
			select {
			case out <- c:
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

func (a *Adapter) decorate(r *http.Request) {
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-api-key", a.apiKey)
	r.Header.Set("anthropic-version", a.version)
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
