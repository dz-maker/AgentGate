package openai

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

type streamResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Delta struct {
			Role      types.Role            `json:"role,omitempty"`
			Content   any                   `json:"content,omitempty"`
			ToolCalls []types.ToolCallDelta `json:"tool_calls,omitempty"`
		} `json:"delta"`
		FinishReason any `json:"finish_reason"`
	} `json:"choices"`
	Usage *types.Usage `json:"usage,omitempty"`
}

func readSSE(ctx context.Context, r io.Reader, onData func([]byte) bool) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var ev bytes.Buffer

	flush := func() error {
		if ev.Len() == 0 {
			return nil
		}
		data := bytes.TrimSpace(ev.Bytes())
		ev.Reset()
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			return nil
		}
		if !onData(data) {
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
		if strings.HasPrefix(line, "data:") {
			if ev.Len() > 0 {
				ev.WriteByte('\n')
			}
			ev.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return flush()
}

func translateChunk(data []byte) (types.Chunk, bool) {
	var sr streamResponse
	if err := json.Unmarshal(data, &sr); err != nil {
		return types.Chunk{}, false
	}
	chunk := types.Chunk{
		ID:        sr.ID,
		Model:     sr.Model,
		Usage:     sr.Usage,
		Raw:       append([]byte(nil), data...),
		CreatedAt: time.Now(),
	}
	if len(sr.Choices) == 0 {
		return chunk, true
	}
	choice := sr.Choices[0]
	switch c := choice.Delta.Content.(type) {
	case string:
		chunk.Content = c
	case nil:
	default:
		raw, _ := json.Marshal(c)
		chunk.Content = string(raw)
	}
	chunk.ToolCalls = choice.Delta.ToolCalls
	if r, ok := choice.FinishReason.(string); ok {
		chunk.FinishReason = r
	}
	return chunk, true
}
