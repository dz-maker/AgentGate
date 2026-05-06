package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

// buildPayload converts AgentGate's internal Request into Anthropic's
// /v1/messages payload. The structural difference: Anthropic separates
// system from messages, and tool calls round-trip through "tool_use" /
// "tool_result" content blocks rather than tool_calls/tool messages.
//
// We honor cache_control.prefix_hint == "share_max" by tagging the system
// block + tool defs as ephemeral so Anthropic's prompt cache kicks in
// across calls in the same window. This is the gateway's only knob into
// Anthropic-side prefix caching.
func buildPayload(req *types.Request, stream bool) map[string]any {
	payload := map[string]any{
		"model":  req.Model,
		"stream": stream,
	}
	if req.MaxTokens != nil {
		payload["max_tokens"] = *req.MaxTokens
	} else {
		// Anthropic requires max_tokens; pick a generous default rather
		// than refusing the request.
		payload["max_tokens"] = 1024
	}
	if req.Temperature != nil {
		payload["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		payload["top_p"] = *req.TopP
	}

	enableCache := req.CacheControl.PrefixHint == "share_max"

	system, msgs := splitSystem(req.Messages, enableCache)
	if system != nil {
		payload["system"] = system
	}
	payload["messages"] = msgs

	if len(req.Tools) > 0 {
		tools := make([]any, 0, len(req.Tools))
		for _, t := range req.Tools {
			obj := map[string]any{}
			if len(t.Function) > 0 {
				_ = json.Unmarshal(t.Function, &obj)
			}
			anth := map[string]any{
				"name":        obj["name"],
				"description": obj["description"],
			}
			if schema, ok := obj["parameters"]; ok {
				anth["input_schema"] = schema
			}
			if enableCache {
				anth["cache_control"] = map[string]string{"type": "ephemeral"}
			}
			tools = append(tools, anth)
		}
		payload["tools"] = tools
		if tc := translateAnthropicToolChoice(req.ToolChoice); tc != nil {
			payload["tool_choice"] = tc
		}
	}

	if len(req.Stop) > 0 {
		payload["stop_sequences"] = req.Stop
	}
	return payload
}

func splitSystem(in []types.Message, enableCache bool) (any, []map[string]any) {
	var systemParts []string
	out := make([]map[string]any, 0, len(in))
	for _, m := range in {
		if m.Role == types.RoleSystem {
			systemParts = append(systemParts, m.ContentString())
			continue
		}
		out = append(out, anthropicMessage(m))
	}
	if len(systemParts) == 0 {
		return nil, out
	}
	combined := strings.Join(systemParts, "\n\n")
	if enableCache {
		return []map[string]any{{
			"type":          "text",
			"text":          combined,
			"cache_control": map[string]string{"type": "ephemeral"},
		}}, out
	}
	return combined, out
}

func anthropicMessage(m types.Message) map[string]any {
	role := string(m.Role)
	if role == "tool" {
		// Tool result -> user message with tool_result content block
		return map[string]any{
			"role": "user",
			"content": []map[string]any{{
				"type":        "tool_result",
				"tool_use_id": m.ToolCallID,
				"content":     m.ContentString(),
			}},
		}
	}
	if role == string(types.RoleAssistant) && len(m.ToolCalls) > 0 {
		blocks := []map[string]any{}
		if text := m.ContentString(); text != "" {
			blocks = append(blocks, map[string]any{"type": "text", "text": text})
		}
		for _, tc := range m.ToolCalls {
			var args any
			if tc.Function.Arguments != "" {
				if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
					args = tc.Function.Arguments
				}
			}
			blocks = append(blocks, map[string]any{
				"type":  "tool_use",
				"id":    tc.ID,
				"name":  tc.Function.Name,
				"input": args,
			})
		}
		return map[string]any{"role": "assistant", "content": blocks}
	}
	return map[string]any{"role": role, "content": m.ContentString()}
}

type messagesResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Content []struct {
		Type  string          `json:"type"`
		Text  string          `json:"text,omitempty"`
		ID    string          `json:"id,omitempty"`
		Name  string          `json:"name,omitempty"`
		Input json.RawMessage `json:"input,omitempty"`
	} `json:"content"`
	StopReason string `json:"stop_reason"`
	Usage      struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

func (m messagesResponse) toResponse(model string) *types.Response {
	resp := &types.Response{
		ID:      m.ID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
	}
	choice := types.Choice{
		Index:        0,
		Message:      types.Message{Role: types.RoleAssistant},
		FinishReason: stopReasonToFinish(m.StopReason),
	}
	var textBuf strings.Builder
	for _, blk := range m.Content {
		switch blk.Type {
		case "text":
			textBuf.WriteString(blk.Text)
		case "tool_use":
			choice.Message.ToolCalls = append(choice.Message.ToolCalls, types.ToolCallDelta{
				ID:   blk.ID,
				Type: "function",
				Function: types.ToolCallFunction{
					Name:      blk.Name,
					Arguments: string(blk.Input),
				},
			})
		}
	}
	if textBuf.Len() > 0 {
		choice.Message.Content = textBuf.String()
	}
	resp.Choices = []types.Choice{choice}
	if m.Usage.InputTokens > 0 || m.Usage.OutputTokens > 0 {
		resp.Usage = &types.Usage{
			PromptTokens:     m.Usage.InputTokens,
			CompletionTokens: m.Usage.OutputTokens,
			TotalTokens:      m.Usage.InputTokens + m.Usage.OutputTokens,
		}
	}
	return resp
}

func stopReasonToFinish(reason string) string {
	switch reason {
	case "end_turn":
		return "stop"
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	default:
		return reason
	}
}

// readMessagesSSE consumes Anthropic's event-stream and emits AgentGate
// chunks. It forwards content_block_delta and message_delta usage, and
// captures input_tokens out of message_start so streaming responses
// surface PromptTokens (Anthropic only ships output_tokens on the delta).
func readMessagesSSE(ctx context.Context, r io.Reader, model string, send func(types.Chunk) bool) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var event string
	var data bytes.Buffer
	var inputTokens int

	flush := func() error {
		if data.Len() == 0 {
			event = ""
			return nil
		}
		raw := bytes.TrimSpace(data.Bytes())
		data.Reset()
		ev := event
		event = ""
		if len(raw) == 0 {
			return nil
		}
		if ev == "message_start" {
			if n, ok := parseMessageStartInputTokens(raw); ok {
				inputTokens = n
			}
			return nil
		}
		chunk, ok := translateAnthropicEvent(ev, raw, model)
		if !ok {
			return nil
		}
		if chunk.Usage != nil && inputTokens > 0 {
			chunk.Usage.PromptTokens = inputTokens
			chunk.Usage.TotalTokens = inputTokens + chunk.Usage.CompletionTokens
		}
		if !send(chunk) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return errors.New("sse consumer stopped")
		}
		return nil
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return flush()
}

type contentDeltaEvent struct {
	Index int `json:"index"`
	Delta struct {
		Type        string          `json:"type"`
		Text        string          `json:"text,omitempty"`
		PartialJSON string          `json:"partial_json,omitempty"`
		Input       json.RawMessage `json:"input,omitempty"`
	} `json:"delta"`
}

type messageDeltaEvent struct {
	Delta struct {
		StopReason string `json:"stop_reason"`
	} `json:"delta"`
	Usage struct {
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// translateAnthropicToolChoice maps OpenAI-style tool_choice values to
// Anthropic's shape. OpenAI accepts "auto" / "none" / "required" /
// {"type":"function","function":{"name":"X"}}; Anthropic wants
// {"type":"auto"|"any"|"tool", "name":"X"} (no equivalent for "none" —
// the caller should just omit tools instead). Returns nil to leave the
// payload unset.
func translateAnthropicToolChoice(raw any) any {
	if raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case string:
		switch v {
		case "", "auto":
			return nil
		case "none":
			return nil
		case "required", "any":
			return map[string]string{"type": "any"}
		}
	case map[string]any:
		if name, _ := v["name"].(string); name != "" {
			return map[string]string{"type": "tool", "name": name}
		}
		if fn, ok := v["function"].(map[string]any); ok {
			if name, _ := fn["name"].(string); name != "" {
				return map[string]string{"type": "tool", "name": name}
			}
		}
	}
	return nil
}

// parseMessageStartInputTokens pulls usage.input_tokens out of an
// Anthropic message_start frame. Anthropic emits this once per response;
// subsequent message_delta frames only carry output_tokens, so the
// gateway must remember the input count to compute total usage.
func parseMessageStartInputTokens(raw []byte) (int, bool) {
	var ev struct {
		Message struct {
			Usage struct {
				InputTokens int `json:"input_tokens"`
			} `json:"usage"`
		} `json:"message"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return 0, false
	}
	if ev.Message.Usage.InputTokens <= 0 {
		return 0, false
	}
	return ev.Message.Usage.InputTokens, true
}

func translateAnthropicEvent(event string, raw []byte, model string) (types.Chunk, bool) {
	switch event {
	case "content_block_delta":
		var ev contentDeltaEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return types.Chunk{}, false
		}
		switch ev.Delta.Type {
		case "text_delta":
			return types.Chunk{
				ID:        "chatcmpl_anthropic",
				Model:     model,
				Content:   ev.Delta.Text,
				CreatedAt: time.Now(),
			}, true
		case "input_json_delta":
			// Tool argument delta. AgentGate's stream parser prefers the
			// final tool_use block, so we surface a tool-call delta with
			// partial arguments and let downstream re-buffer.
			return types.Chunk{
				ID:        "chatcmpl_anthropic",
				Model:     model,
				CreatedAt: time.Now(),
				ToolCalls: []types.ToolCallDelta{{
					Index: ev.Index,
					Function: types.ToolCallFunction{
						Arguments: ev.Delta.PartialJSON,
					},
				}},
			}, true
		}
	case "message_delta":
		var ev messageDeltaEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return types.Chunk{}, false
		}
		return types.Chunk{
			ID:           "chatcmpl_anthropic",
			Model:        model,
			FinishReason: stopReasonToFinish(ev.Delta.StopReason),
			Usage: &types.Usage{
				CompletionTokens: ev.Usage.OutputTokens,
				TotalTokens:      ev.Usage.OutputTokens,
			},
			CreatedAt: time.Now(),
		}, true
	case "message_stop":
		return types.Chunk{ID: "chatcmpl_anthropic", Model: model, FinishReason: "stop", CreatedAt: time.Now()}, true
	}
	return types.Chunk{}, false
}
