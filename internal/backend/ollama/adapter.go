package ollama

import (
	"bufio"
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
	name     string
	endpoint string
	client   *http.Client
	headers  map[string]string

	healthy   atomic.Bool
	inFlight  atomic.Int64
	total     atomic.Uint64
	failed    atomic.Uint64
	mu        sync.RWMutex
	lastError string
	lastSeen  time.Time
}

type Options struct {
	Name          string
	Endpoint      string
	Headers       map[string]string
	HeaderTimeout time.Duration
}

func New(opts Options) (*Adapter, error) {
	if opts.Name == "" {
		return nil, errors.New("ollama backend name is required")
	}
	if opts.Endpoint == "" {
		return nil, errors.New("ollama backend endpoint is required")
	}
	a := &Adapter{
		name:     opts.Name,
		endpoint: strings.TrimRight(opts.Endpoint, "/"),
		client:   httpx.NewClient(httpx.Options{HeaderTimeout: opts.HeaderTimeout}),
		headers:  opts.Headers,
		lastSeen: time.Now(),
	}
	a.healthy.Store(true)
	return a, nil
}

func (a *Adapter) Name() string { return a.name }

func (a *Adapter) Capabilities() types.Capabilities {
	return types.Capabilities{
		Vendor:              "ollama",
		SupportsStreaming:   true,
		SupportsToolCalling: true, // Ollama 0.3+ supports tool calls
		SupportsAbort:       false,
		PrefixCacheMode:     types.PrefixCacheNone,
		KVProvider:          "none",
	}
}

func (a *Adapter) Healthy() bool { return a.healthy.Load() }

func (a *Adapter) Stats() types.BackendStats {
	a.mu.RLock()
	lastErr, lastSeen := a.lastError, a.lastSeen
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

// SelectInstance is a no-op for single-instance backends but lets the router
// surface the instance ID consistently across adapters.
func (a *Adapter) SelectInstance(_ context.Context, _ backend.RoutingHint) (string, error) {
	if !a.healthy.Load() {
		return "", backend.ErrNoHealthyBackend
	}
	return a.name + "-0", nil
}

func (a *Adapter) Complete(ctx context.Context, req *types.Request) (*types.Response, error) {
	a.begin()
	defer a.end()

	body, err := json.Marshal(toOllamaPayload(req, false))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint+"/api/chat", bytes.NewReader(body))
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
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		err := fmt.Errorf("ollama status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		a.fail(err)
		return nil, err
	}

	var nd ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&nd); err != nil {
		a.fail(err)
		return nil, err
	}
	a.success()
	return ollamaToResponse(req.Model, nd), nil
}

func (a *Adapter) Stream(ctx context.Context, req *types.Request) (<-chan types.Chunk, error) {
	body, err := json.Marshal(toOllamaPayload(req, true))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	a.decorate(httpReq)

	a.begin()
	resp, err := a.client.Do(httpReq)
	if err != nil {
		a.fail(err)
		a.end()
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		err := fmt.Errorf("ollama stream status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		a.fail(err)
		a.end()
		return nil, err
	}

	out := make(chan types.Chunk, 32)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		defer a.end()

		// Ollama streams newline-delimited JSON, not SSE.
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			var ev ollamaChatResponse
			if err := json.Unmarshal(line, &ev); err != nil {
				continue
			}
			chunk := types.Chunk{
				ID:        "chatcmpl_ollama",
				Model:     req.Model,
				Content:   ev.Message.Content,
				ToolCalls: ev.Message.ToolCalls,
				CreatedAt: time.Now(),
			}
			if ev.Done {
				chunk.FinishReason = "stop"
				if len(ev.Message.ToolCalls) > 0 {
					chunk.FinishReason = "tool_calls"
				}
				if ev.PromptEvalCount > 0 || ev.EvalCount > 0 {
					chunk.Usage = &types.Usage{
						PromptTokens:     ev.PromptEvalCount,
						CompletionTokens: ev.EvalCount,
						TotalTokens:      ev.PromptEvalCount + ev.EvalCount,
					}
				}
			}
			select {
			case out <- chunk:
			case <-ctx.Done():
				return
			}
			if ev.Done {
				a.success()
				return
			}
		}
		if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
			a.fail(err)
			return
		}
		a.success()
	}()
	return out, nil
}

func (a *Adapter) decorate(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	for k, v := range a.headers {
		req.Header.Set(k, v)
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
	a.lastSeen = time.Now()
	a.lastError = ""
	a.mu.Unlock()
	a.healthy.Store(true)
}

func (a *Adapter) fail(err error) {
	a.failed.Add(1)
	a.mu.Lock()
	a.lastSeen = time.Now()
	if err != nil {
		a.lastError = err.Error()
	}
	a.mu.Unlock()
	// Single-instance: only flip unhealthy on a clearly unrecoverable
	// failure, otherwise keep serving and let the next request retry.
	if isFatalNetworkError(err) {
		a.healthy.Store(false)
	}
}

func isFatalNetworkError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") || strings.Contains(msg, "no such host")
}
