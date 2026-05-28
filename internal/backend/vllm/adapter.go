package vllm

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
	"github.com/agentgate/agentgate/internal/parser"
	"github.com/agentgate/agentgate/pkg/types"
)

const (
	instanceFailureLimit         = 3
	instanceRecoverySuccessLimit = 2
)

type Adapter struct {
	name      string
	client    *http.Client
	headers   map[string]string
	instances []*Instance
	// byID is populated only in New and is not modified afterwards; reads are
	// safe without locks.
	byID   map[string]*Instance
	rr     atomic.Uint64
	stopCh chan struct{}

	// vendor + prefixMode override what we report in Capabilities. They are
	// optional so the default vLLM caller does not need to set them; SGLang
	// and other OpenAI-compatible engines reuse this adapter and override
	// these to surface the right cache semantics to the router.
	vendor     string
	prefixMode types.PrefixCacheMode
	kvProvider string
	models     []string
}

type Instance struct {
	ID       string
	Endpoint string

	healthy   atomic.Bool
	inFlight  atomic.Int64
	total     atomic.Uint64
	failed    atomic.Uint64
	hitHints  atomic.Uint64
	missHints atomic.Uint64

	mu        sync.RWMutex
	lastSeen  time.Time
	lastError string

	failureStreak int
	successStreak int
}

type Options struct {
	Name           string
	Endpoints      []string
	Headers        map[string]string
	HeaderTimeout  time.Duration
	HealthTimeout  time.Duration
	HealthInterval time.Duration

	// Vendor / PrefixMode / KVProvider override capability advertisement.
	// SGLang reuses this adapter with Vendor="sglang" and
	// PrefixMode=PrefixCacheRadix so the router can model RadixAttention
	// without us forking the entire HTTP client.
	Vendor     string
	PrefixMode types.PrefixCacheMode
	KVProvider string

	// Models is the list of model IDs this cluster serves. Surfaced via
	// Capabilities().SupportedModels so the router can do model-affinity
	// checks before dispatch. Optional; empty means "advertise nothing,
	// router treats it as a wildcard."
	Models []string
}

func New(opts Options) (*Adapter, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("vllm backend name is required")
	}
	if len(opts.Endpoints) == 0 {
		return nil, fmt.Errorf("vllm backend %q needs at least one endpoint", opts.Name)
	}
	if opts.HeaderTimeout <= 0 {
		opts.HeaderTimeout = 30 * time.Second
	}

	vendor := opts.Vendor
	if vendor == "" {
		vendor = "vllm"
	}
	prefixMode := opts.PrefixMode
	if prefixMode == "" {
		prefixMode = types.PrefixCacheAPC
	}
	kvProvider := opts.KVProvider
	if kvProvider == "" {
		kvProvider = "native"
	}
	a := &Adapter{
		name:       opts.Name,
		client:     httpx.NewClient(httpx.Options{HeaderTimeout: opts.HeaderTimeout}),
		headers:    opts.Headers,
		byID:       map[string]*Instance{},
		stopCh:     make(chan struct{}),
		vendor:     vendor,
		prefixMode: prefixMode,
		kvProvider: kvProvider,
		models:     append([]string(nil), opts.Models...),
	}
	for i, endpoint := range opts.Endpoints {
		endpoint = strings.TrimRight(endpoint, "/")
		inst := &Instance{
			ID:       fmt.Sprintf("%s-%d", opts.Name, i),
			Endpoint: endpoint,
			lastSeen: time.Now(),
		}
		inst.healthy.Store(true)
		a.instances = append(a.instances, inst)
		a.byID[inst.ID] = inst
	}

	if opts.HealthInterval > 0 {
		go a.healthLoop(opts.HealthInterval, opts.HealthTimeout)
	}

	return a, nil
}

func (a *Adapter) Name() string {
	return a.name
}

func (a *Adapter) Complete(ctx context.Context, req *types.Request) (*types.Response, error) {
	inst, err := a.instanceFor(ctx, req.DesiredInstance)
	if err != nil {
		return nil, err
	}

	payload, err := a.buildPayload(req, false)
	if err != nil {
		return nil, err
	}

	body, err := a.do(ctx, inst, payload)
	if err != nil {
		return nil, err
	}

	var out types.Response
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode vllm response: %w", err)
	}
	if out.Object == "" {
		out.Object = "chat.completion"
	}
	return &out, nil
}

func (a *Adapter) Stream(ctx context.Context, req *types.Request) (<-chan types.Chunk, error) {
	inst, err := a.instanceFor(ctx, req.DesiredInstance)
	if err != nil {
		return nil, err
	}

	payload, err := a.buildPayload(req, true)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, inst.Endpoint+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	a.decorate(httpReq)

	inst.begin()
	resp, err := a.client.Do(httpReq)
	if err != nil {
		inst.fail(err)
		inst.end()
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		err := fmt.Errorf("vllm stream status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		inst.fail(err)
		inst.end()
		return nil, err
	}

	out := make(chan types.Chunk, 32)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		defer inst.end()

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
				// AgentGate may cancel the upstream context deliberately after a
				// tool call is complete. That is a successful gateway decision, not
				// evidence that the vLLM instance is unhealthy.
				inst.success()
				return
			}
			inst.fail(err)
			return
		}
		inst.success()
	}()

	return out, nil
}

func (a *Adapter) SelectInstance(ctx context.Context, hint backend.RoutingHint) (string, error) {
	if hint.PreferredInstance != "" {
		if inst := a.byID[hint.PreferredInstance]; inst != nil && inst.isHealthy() {
			inst.hitHints.Add(1)
			return inst.ID, nil
		}
	}

	inst, err := a.pickHealthy()
	if err != nil {
		return "", err
	}
	if hint.PreferredInstance != "" {
		inst.missHints.Add(1)
	}
	return inst.ID, nil
}

func (a *Adapter) Capabilities() types.Capabilities {
	return types.Capabilities{
		Vendor:                   a.vendor,
		SupportsPrefixCache:      true,
		SupportsStructuredOutput: true,
		SupportsLogprobs:         true,
		SupportsStreaming:        true,
		SupportsToolCalling:      true,
		SupportsAbort:            true,
		PrefixCacheMode:          a.prefixMode,
		KVProvider:               a.kvProvider,
		SupportedModels:          a.models,
	}
}

func (a *Adapter) Healthy() bool {
	for _, inst := range a.instances {
		if inst.isHealthy() {
			return true
		}
	}
	return false
}

func (a *Adapter) Stats() types.BackendStats {
	stats := types.BackendStats{
		Name:             a.name,
		Healthy:          a.Healthy(),
		PrefixCacheAware: true,
	}
	for _, inst := range a.instances {
		inFlight := inst.inFlight.Load()
		total := inst.total.Load()
		failed := inst.failed.Load()
		stats.InFlight += inFlight
		stats.TotalRequests += total
		stats.FailedRequests += failed

		inst.mu.RLock()
		lastSeen := inst.lastSeen
		lastErr := inst.lastError
		inst.mu.RUnlock()

		stats.Instances = append(stats.Instances, types.InstanceStats{
			ID:              inst.ID,
			Endpoint:        inst.Endpoint,
			Healthy:         inst.isHealthy(),
			InFlight:        inFlight,
			TotalRequests:   total,
			FailedRequests:  failed,
			LastSeen:        lastSeen,
			LastError:       lastErr,
			PrefixHitHints:  inst.hitHints.Load(),
			PrefixMissHints: inst.missHints.Load(),
		})
		if lastErr != "" {
			stats.LastError = lastErr
		}
	}
	return stats
}

func (a *Adapter) Close() error {
	select {
	case <-a.stopCh:
	default:
		close(a.stopCh)
	}
	if transport, ok := a.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	return nil
}

func (a *Adapter) instanceFor(ctx context.Context, desired string) (*Instance, error) {
	if desired != "" {
		if inst := a.byID[desired]; inst != nil && inst.isHealthy() {
			return inst, nil
		}
	}
	return a.pickHealthy()
}

func (a *Adapter) pickHealthy() (*Instance, error) {
	if len(a.instances) == 0 {
		return nil, backend.ErrNoHealthyBackend
	}
	start := int(a.rr.Add(1) % uint64(len(a.instances)))
	for i := 0; i < len(a.instances); i++ {
		inst := a.instances[(start+i)%len(a.instances)]
		if inst.isHealthy() {
			return inst, nil
		}
	}
	return nil, backend.ErrNoHealthyBackend
}

func (a *Adapter) buildPayload(req *types.Request, stream bool) (map[string]any, error) {
	payload := map[string]any{}
	if len(req.Raw) > 0 {
		if err := json.Unmarshal(req.Raw, &payload); err != nil {
			return nil, err
		}
	}
	if len(payload) == 0 {
		payload["model"] = req.Model
		payload["messages"] = req.Messages
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
	}
	delete(payload, "x_agentgate")
	payload["stream"] = stream

	stops := normalizeStop(payload["stop"])
	stops = append(stops, req.Stop...)
	if len(req.Tools) > 0 {
		stops = append(stops, parser.StopStringsForModel(req.Model)...)
	}
	if len(stops) > 0 {
		payload["stop"] = dedupe(stops)
	}

	return payload, nil
}

func (a *Adapter) do(ctx context.Context, inst *Instance, payload map[string]any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, inst.Endpoint+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	a.decorate(httpReq)

	inst.begin()
	defer inst.end()

	resp, err := a.client.Do(httpReq)
	if err != nil {
		inst.fail(err)
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
	if err != nil {
		inst.fail(err)
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("vllm status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		inst.fail(err)
		return nil, err
	}
	inst.success()
	return data, nil
}

func (a *Adapter) decorate(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for k, v := range a.headers {
		req.Header.Set(k, v)
	}
}

func (a *Adapter) healthLoop(interval, timeout time.Duration) {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, inst := range a.instances {
				go a.checkHealth(inst, timeout)
			}
		case <-a.stopCh:
			return
		}
	}
}

func (a *Adapter) checkHealth(inst *Instance, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, inst.Endpoint+"/health", nil)
	if err != nil {
		inst.fail(err)
		return
	}
	a.decorate(req)
	resp, err := a.client.Do(req)
	if err != nil {
		inst.fail(err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 500 {
		inst.success()
		return
	}
	inst.fail(fmt.Errorf("health status %d", resp.StatusCode))
}

func (i *Instance) begin() {
	i.inFlight.Add(1)
	i.total.Add(1)
	i.mu.Lock()
	i.lastSeen = time.Now()
	i.mu.Unlock()
}

func (i *Instance) end() {
	i.inFlight.Add(-1)
}

func (i *Instance) success() {
	i.mu.Lock()
	i.lastSeen = time.Now()
	i.failureStreak = 0
	i.successStreak++
	if i.healthy.Load() || i.successStreak >= instanceRecoverySuccessLimit {
		i.healthy.Store(true)
		i.lastError = ""
	}
	i.mu.Unlock()
}

func (i *Instance) fail(err error) {
	i.failed.Add(1)
	i.mu.Lock()
	i.lastSeen = time.Now()
	i.failureStreak++
	i.successStreak = 0
	if err != nil {
		i.lastError = err.Error()
	}
	if i.failureStreak >= instanceFailureLimit {
		i.healthy.Store(false)
	}
	i.mu.Unlock()
}

func (i *Instance) isHealthy() bool {
	return i.healthy.Load()
}

func normalizeStop(v any) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return []string{t}
	case []string:
		return append([]string(nil), t...)
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, item := range in {
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	return out
}
