package mock

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

const mockMessage = "AgentGate mock backend is running. Point this config at vLLM when you are ready."

type Backend struct {
	name  string
	total atomic.Uint64
}

func New(name string) *Backend {
	if name == "" {
		name = "mock"
	}
	return &Backend{name: name}
}

func (b *Backend) Name() string {
	return b.name
}

func (b *Backend) Complete(ctx context.Context, req *types.Request) (*types.Response, error) {
	b.total.Add(1)
	return &types.Response{
		ID:      "chatcmpl_mock",
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []types.Choice{{
			Index: 0,
			Message: types.Message{
				Role:    types.RoleAssistant,
				Content: mockMessage,
			},
			FinishReason: "stop",
		}},
	}, nil
}

func (b *Backend) Stream(ctx context.Context, req *types.Request) (<-chan types.Chunk, error) {
	b.total.Add(1)
	out := make(chan types.Chunk, 4)
	go func() {
		defer close(out)
		parts := strings.Split(mockMessage, " ")
		for _, part := range parts {
			select {
			case out <- types.Chunk{ID: "chatcmpl_mock", Model: req.Model, Content: part + " ", CreatedAt: time.Now()}:
			case <-ctx.Done():
				return
			}
		}
		out <- types.Chunk{ID: "chatcmpl_mock", Model: req.Model, FinishReason: "stop", CreatedAt: time.Now()}
	}()
	return out, nil
}

func (b *Backend) Capabilities() types.Capabilities {
	return types.Capabilities{
		Vendor:              "mock",
		SupportsStreaming:   true,
		SupportsToolCalling: true,
		PrefixCacheMode:     types.PrefixCacheNone,
	}
}

func (b *Backend) Healthy() bool {
	return true
}

func (b *Backend) Stats() types.BackendStats {
	return types.BackendStats{
		Name:          b.name,
		Healthy:       true,
		TotalRequests: b.total.Load(),
		Instances: []types.InstanceStats{{
			ID:       b.name + "-0",
			Endpoint: "mock://local",
			Healthy:  true,
		}},
	}
}
