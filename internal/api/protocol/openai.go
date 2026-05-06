package protocol

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

type ChatCompletionRequest struct {
	Model          string                 `json:"model"`
	Messages       []types.Message        `json:"messages"`
	Tools          []types.ToolDefinition `json:"tools,omitempty"`
	Temperature    *float64               `json:"temperature,omitempty"`
	TopP           *float64               `json:"top_p,omitempty"`
	MaxTokens      *int                   `json:"max_tokens,omitempty"`
	Stop           any                    `json:"stop,omitempty"`
	Stream         bool                   `json:"stream,omitempty"`
	ResponseFormat json.RawMessage        `json:"response_format,omitempty"`
	ToolChoice     any                    `json:"tool_choice,omitempty"`
	AgentGate      types.AgentGateOptions `json:"x_agentgate,omitempty"`
}

func (r ChatCompletionRequest) Normalize(raw json.RawMessage) (types.Request, error) {
	stop, err := normalizeStop(r.Stop)
	if err != nil {
		return types.Request{}, err
	}

	req := types.Request{
		Model:          r.Model,
		Messages:       r.Messages,
		Tools:          r.Tools,
		Temperature:    r.Temperature,
		TopP:           r.TopP,
		MaxTokens:      r.MaxTokens,
		Stop:           stop,
		Stream:         r.Stream,
		ResponseFormat: r.ResponseFormat,
		ToolChoice:     r.ToolChoice,
		SessionID:      r.AgentGate.SessionID,
		TenantID:       r.AgentGate.TenantID,
		AgentID:        r.AgentGate.AgentID,
		TraceID:        r.AgentGate.TraceID,
		StepID:         r.AgentGate.StepID,
		ParentStepID:   r.AgentGate.ParentStepID,
		StepType:       r.AgentGate.StepType,
		PrefixHash:     r.AgentGate.PrefixHash,
		CacheControl:   r.AgentGate.CacheControl,
		Raw:            raw,
	}
	if req.TenantID == "" {
		req.TenantID = "default"
	}
	return req, nil
}

func normalizeStop(v any) ([]string, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return []string{t}, nil
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("stop must be string or []string")
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("stop must be string or []string")
	}
}

func ChunkFromBackend(chunk types.Chunk) types.Response {
	if len(chunk.ToolCalls) > 0 || chunk.FinishReason != "" {
		return types.Response{
			ID:      chunk.ID,
			Object:  "chat.completion.chunk",
			Created: time.Now().Unix(),
			Model:   chunk.Model,
			Choices: []types.Choice{{
				Index: 0,
				Delta: types.Delta{
					Content:   chunk.Content,
					ToolCalls: chunk.ToolCalls,
				},
				FinishReason: chunk.FinishReason,
			}},
			Usage: chunk.Usage,
		}
	}

	return types.Response{
		ID:      chunk.ID,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   chunk.Model,
		Choices: []types.Choice{{
			Index: 0,
			Delta: types.Delta{
				Content: chunk.Content,
			},
		}},
		Usage: chunk.Usage,
	}
}

func ToolCallChunk(id, model string, tc types.ToolCall) types.Response {
	return types.Response{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []types.Choice{{
			Index: 0,
			Delta: types.Delta{
				ToolCalls: []types.ToolCallDelta{{
					Index: 0,
					ID:    tc.ID,
					Type:  "function",
					Function: types.ToolCallFunction{
						Name:      tc.Name,
						Arguments: string(tc.Arguments),
					},
				}},
			},
		}},
	}
}

func FinishChunk(id, model, reason string) types.Response {
	return types.Response{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []types.Choice{{
			Index:        0,
			Delta:        types.Delta{},
			FinishReason: reason,
		}},
	}
}
