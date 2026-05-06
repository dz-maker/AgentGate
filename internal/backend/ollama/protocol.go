package ollama

import (
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

type ollamaMessage struct {
	Role      string                `json:"role"`
	Content   string                `json:"content,omitempty"`
	ToolCalls []types.ToolCallDelta `json:"tool_calls,omitempty"`
}

type ollamaChatResponse struct {
	Model           string        `json:"model"`
	CreatedAt       time.Time     `json:"created_at"`
	Message         ollamaMessage `json:"message"`
	Done            bool          `json:"done"`
	PromptEvalCount int           `json:"prompt_eval_count,omitempty"`
	EvalCount       int           `json:"eval_count,omitempty"`
}

func toOllamaPayload(req *types.Request, stream bool) map[string]any {
	msgs := make([]ollamaMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, ollamaMessage{
			Role:    string(m.Role),
			Content: m.ContentString(),
		})
	}
	payload := map[string]any{
		"model":    req.Model,
		"messages": msgs,
		"stream":   stream,
	}
	options := map[string]any{}
	if req.Temperature != nil {
		options["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		options["top_p"] = *req.TopP
	}
	if req.MaxTokens != nil {
		// Ollama calls this "num_predict".
		options["num_predict"] = *req.MaxTokens
	}
	if len(req.Stop) > 0 {
		options["stop"] = req.Stop
	}
	if len(options) > 0 {
		payload["options"] = options
	}
	if len(req.Tools) > 0 {
		payload["tools"] = req.Tools
	}
	return payload
}

func ollamaToResponse(model string, ev ollamaChatResponse) *types.Response {
	resp := &types.Response{
		ID:      "chatcmpl_ollama",
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []types.Choice{{
			Index: 0,
			Message: types.Message{
				Role:      types.RoleAssistant,
				Content:   ev.Message.Content,
				ToolCalls: ev.Message.ToolCalls,
			},
			FinishReason: "stop",
		}},
	}
	if len(ev.Message.ToolCalls) > 0 {
		resp.Choices[0].FinishReason = "tool_calls"
	}
	if ev.PromptEvalCount > 0 || ev.EvalCount > 0 {
		resp.Usage = &types.Usage{
			PromptTokens:     ev.PromptEvalCount,
			CompletionTokens: ev.EvalCount,
			TotalTokens:      ev.PromptEvalCount + ev.EvalCount,
		}
	}
	return resp
}
