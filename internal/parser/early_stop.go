package parser

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/agentgate/agentgate/pkg/types"
)

type EventKind int

const (
	EventNone EventKind = iota
	EventText
	EventToolDetected
	EventToolComplete
	EventAbort
)

type ParseEvent struct {
	Kind       EventKind
	Text       string
	ToolCall   *types.ToolCall
	ShouldStop bool
}

type State int

const (
	StateText State = iota
	StateTool
	StateComplete
	StateAbort
)

type Options struct {
	MaxBufferBytes int
	// AggressiveAbort makes EventAbort surface ShouldStop=true so the
	// caller can terminate the upstream stream rather than continuing
	// to forward raw text after a malformed/oversized tool call. Useful
	// when downstream clients cannot tolerate partial or speculative
	// output once a tool call was detected.
	AggressiveAbort bool
}

type StreamingParser struct {
	state           State
	buffer          string
	marker          string
	maxBufferBytes  int
	longestMarker   int
	aggressiveAbort bool
}

var startMarkers = []string{
	"\n## Tool Call:\n",
	"<|python_tag|>",
	"<tool_call>",
	"<tool_use>",
}

var stopStrings = []string{
	"</tool_call>",
	"</tool_use>",
	"<|eom_id|>",
}

func NewStreamingParser(opts Options) *StreamingParser {
	maxBuffer := opts.MaxBufferBytes
	if maxBuffer <= 0 {
		maxBuffer = 16 * 1024
	}
	longest := 0
	for _, marker := range startMarkers {
		if len(marker) > longest {
			longest = len(marker)
		}
	}
	return &StreamingParser{
		maxBufferBytes:  maxBuffer,
		longestMarker:   longest,
		aggressiveAbort: opts.AggressiveAbort,
	}
}

func StopStringsForModel(model string) []string {
	model = strings.ToLower(model)
	switch {
	case strings.Contains(model, "llama"):
		return []string{"<|eom_id|>"}
	case strings.Contains(model, "claude") || strings.Contains(model, "anthropic"):
		return []string{"</tool_use>", "</tool_call>"}
	case strings.Contains(model, "qwen"):
		return []string{"</tool_call>"}
	case strings.Contains(model, "deepseek"):
		return []string{"\n##"}
	default:
		return append([]string(nil), stopStrings...)
	}
}

func (p *StreamingParser) Feed(chunk string) ParseEvent {
	if chunk == "" {
		return ParseEvent{Kind: EventNone}
	}
	if p.state == StateComplete {
		return ParseEvent{Kind: EventNone, ShouldStop: true}
	}
	if p.state == StateAbort {
		return ParseEvent{Kind: EventText, Text: chunk}
	}

	p.buffer += chunk
	if len(p.buffer) > p.maxBufferBytes {
		text := p.flushAsText()
		p.state = StateAbort
		return ParseEvent{Kind: EventAbort, Text: text, ShouldStop: p.aggressiveAbort}
	}

	if p.state == StateText {
		if idx, marker, ok := findStartMarker(p.buffer); ok {
			text := p.buffer[:idx]
			p.marker = marker
			p.buffer = p.buffer[idx+len(marker):]
			p.state = StateTool
			ev := p.tryCompleteTool()
			if ev.Kind == EventToolComplete {
				ev.Text = text
				return ev
			}
			return ParseEvent{Kind: EventToolDetected, Text: text}
		}

		text := p.flushTextWithLookahead()
		if text == "" {
			return ParseEvent{Kind: EventNone}
		}
		return ParseEvent{Kind: EventText, Text: text}
	}

	return p.tryCompleteTool()
}

func (p *StreamingParser) Flush() string {
	if p.state == StateText || p.state == StateAbort {
		text := p.buffer
		p.buffer = ""
		return text
	}
	if p.state == StateTool {
		return p.flushAsText()
	}
	return ""
}

func (p *StreamingParser) Reset() {
	p.state = StateText
	p.buffer = ""
	p.marker = ""
}

func (p *StreamingParser) tryCompleteTool() ParseEvent {
	if raw := extractFirstJSONObject(p.buffer); raw != nil {
		tc, err := parseToolCall(raw)
		if err != nil {
			text := p.flushAsText()
			p.state = StateAbort
			return ParseEvent{Kind: EventAbort, Text: text, ShouldStop: p.aggressiveAbort}
		}
		p.state = StateComplete
		p.buffer = ""
		return ParseEvent{Kind: EventToolComplete, ToolCall: tc, ShouldStop: true}
	}

	if idx, ok := findStopMarker(p.buffer); ok {
		raw := extractFirstJSONObject(p.buffer[:idx])
		if raw == nil {
			text := p.flushAsText()
			p.state = StateAbort
			return ParseEvent{Kind: EventAbort, Text: text, ShouldStop: p.aggressiveAbort}
		}
		tc, err := parseToolCall(raw)
		if err != nil {
			text := p.flushAsText()
			p.state = StateAbort
			return ParseEvent{Kind: EventAbort, Text: text, ShouldStop: p.aggressiveAbort}
		}
		p.state = StateComplete
		p.buffer = ""
		return ParseEvent{Kind: EventToolComplete, ToolCall: tc, ShouldStop: true}
	}

	return ParseEvent{Kind: EventNone}
}

func (p *StreamingParser) flushTextWithLookahead() string {
	keep := p.longestMarker - 1
	if keep < 0 {
		keep = 0
	}
	if len(p.buffer) <= keep {
		return ""
	}
	text := p.buffer[:len(p.buffer)-keep]
	p.buffer = p.buffer[len(p.buffer)-keep:]
	return text
}

func (p *StreamingParser) flushAsText() string {
	text := p.buffer
	if p.marker != "" {
		text = p.marker + text
	}
	p.buffer = ""
	p.marker = ""
	return text
}

func findStartMarker(s string) (int, string, bool) {
	bestIdx := -1
	bestMarker := ""
	for _, marker := range startMarkers {
		idx := strings.Index(s, marker)
		if idx < 0 {
			continue
		}
		if bestIdx == -1 || idx < bestIdx || (idx == bestIdx && len(marker) > len(bestMarker)) {
			bestIdx = idx
			bestMarker = marker
		}
	}
	return bestIdx, bestMarker, bestIdx >= 0
}

func findStopMarker(s string) (int, bool) {
	bestIdx := -1
	for _, marker := range stopStrings {
		idx := strings.Index(s, marker)
		if idx < 0 {
			continue
		}
		if bestIdx == -1 || idx < bestIdx {
			bestIdx = idx
		}
	}
	return bestIdx, bestIdx >= 0
}

func extractFirstJSONObject(s string) json.RawMessage {
	// Tool call formats often wrap JSON in text markers such as
	// "<tool_call>{...}</tool_call>". IncrementalJSON intentionally scans to the
	// first '{' instead of requiring the buffer to start with JSON.
	var p IncrementalJSON
	raw, ok := p.Feed(s)
	if !ok {
		return nil
	}
	return raw
}

func parseToolCall(raw json.RawMessage) (*types.ToolCall, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}

	name := readString(obj, "name", "tool", "tool_name")
	if name == "" {
		if fnRaw := obj["function"]; fnRaw != nil {
			var fn map[string]json.RawMessage
			if err := json.Unmarshal(fnRaw, &fn); err == nil {
				name = readString(fn, "name")
				if args := firstRaw(fn, "arguments", "input", "parameters"); args != nil {
					obj["arguments"] = args
				}
			}
		}
	}
	if name == "" {
		return nil, errors.New("tool call missing name")
	}

	args := firstRaw(obj, "arguments", "input", "parameters", "args")
	if args == nil {
		args = json.RawMessage(`{}`)
	}

	var argsString string
	if err := json.Unmarshal(args, &argsString); err == nil {
		args = json.RawMessage(argsString)
	}
	if !json.Valid(args) {
		args = marshalIgnoreError(map[string]string{"value": string(args)})
	}

	return &types.ToolCall{
		ID:        "call_" + randomHex(8),
		Name:      name,
		Arguments: args,
	}, nil
}

func readString(obj map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		raw := obj[key]
		if raw == nil {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s
		}
	}
	return ""
}

func firstRaw(obj map[string]json.RawMessage, keys ...string) json.RawMessage {
	for _, key := range keys {
		if raw := obj[key]; raw != nil {
			return raw
		}
	}
	return nil
}

func marshalIgnoreError(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}
