package parser

import (
	"strings"
	"testing"
)

func TestStreamingParserCompletesToolCallAcrossChunks(t *testing.T) {
	p := NewStreamingParser(Options{MaxBufferBytes: 4096})
	var text string

	ev := p.Feed("I will check <tool")
	text += ev.Text
	if strings.Contains(text, "<tool") {
		t.Fatalf("marker prefix should be held back, got %q", text)
	}

	ev = p.Feed(`_call>{"name":"search","arguments":{"query":"agent gateway"}}</tool_call> trailing`)
	text += ev.Text
	if text != "I will check " {
		t.Fatalf("expected text before tool marker, got %q", text)
	}
	if ev.ToolCall == nil {
		t.Fatal("expected completed tool call")
	}
	if ev.ToolCall.Name != "search" {
		t.Fatalf("expected search tool, got %q", ev.ToolCall.Name)
	}
	if string(ev.ToolCall.Arguments) != `{"query":"agent gateway"}` {
		t.Fatalf("unexpected arguments: %s", ev.ToolCall.Arguments)
	}
	if !ev.ShouldStop {
		t.Fatal("expected early stop decision")
	}
}

func TestStreamingParserFallsBackOnIncompleteToolCall(t *testing.T) {
	p := NewStreamingParser(Options{MaxBufferBytes: 4096})
	ev := p.Feed("before <tool_call>")
	tail := ev.Text + p.Flush()
	if tail != "before <tool_call>" {
		t.Fatalf("expected incomplete tool call to flush as text, got %q", tail)
	}
}

func TestAggressiveAbortSurfacesShouldStopOnBufferOverflow(t *testing.T) {
	// Without aggressive_abort the parser keeps streaming text after a
	// failed tool parse. With it set, the gateway should be told to
	// terminate the upstream stream so partial output stops leaking.
	p := NewStreamingParser(Options{MaxBufferBytes: 16, AggressiveAbort: true})
	ev := p.Feed("<tool_call>{\"name\":\"do_something_with_a_long_arg")
	if ev.Kind != EventAbort {
		t.Fatalf("expected EventAbort on overflow, got %v", ev.Kind)
	}
	if !ev.ShouldStop {
		t.Fatal("aggressive_abort should set ShouldStop on EventAbort")
	}
}

func TestAggressiveAbortDisabledLeavesShouldStopFalse(t *testing.T) {
	// Default (non-aggressive) behavior must remain backwards compatible:
	// an abort should NOT stop the stream; the gateway falls back to
	// forwarding text.
	p := NewStreamingParser(Options{MaxBufferBytes: 16})
	ev := p.Feed("<tool_call>{\"name\":\"do_something_with_a_long_arg")
	if ev.Kind != EventAbort {
		t.Fatalf("expected EventAbort on overflow, got %v", ev.Kind)
	}
	if ev.ShouldStop {
		t.Fatal("non-aggressive abort must not stop the stream")
	}
}

func TestFindStartMarker(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		wantIdx  int
		wantMark string
		wantOK   bool
	}{
		{"no marker", "just text", -1, "", false},
		{"qwen", "before <tool_call>after", 7, "<tool_call>", true},
		{"claude xml", "before <tool_use>", 7, "<tool_use>", true},
		{"llama tag", "x<|python_tag|>y", 1, "<|python_tag|>", true},
		{"earliest wins", "x<tool_use>y<tool_call>", 1, "<tool_use>", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx, mark, ok := findStartMarker(tc.input)
			if idx != tc.wantIdx || mark != tc.wantMark || ok != tc.wantOK {
				t.Fatalf("got (%d, %q, %v), want (%d, %q, %v)", idx, mark, ok, tc.wantIdx, tc.wantMark, tc.wantOK)
			}
		})
	}
}

func TestParseToolCallFormats(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"plain", `{"name":"search","arguments":{"q":"x"}}`, "search"},
		{"function", `{"function":{"name":"lookup","arguments":{"id":1}}}`, "lookup"},
		{"tool name", `{"tool_name":"fetch","input":{"url":"u"}}`, "fetch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseToolCall([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if got.Name != tc.want {
				t.Fatalf("got %q, want %q", got.Name, tc.want)
			}
		})
	}
}

func TestStopStringsForModelUsesModel(t *testing.T) {
	cases := map[string]string{
		"Qwen2.5":     "</tool_call>",
		"llama-3.1":   "<|eom_id|>",
		"claude-3":    "</tool_use>",
		"deepseek-v3": "\n##",
	}
	for model, want := range cases {
		got := strings.Join(StopStringsForModel(model), ",")
		if !strings.Contains(got, want) {
			t.Fatalf("model %s stops %q do not contain %q", model, got, want)
		}
	}
}

func BenchmarkParserFeed(b *testing.B) {
	p := NewStreamingParser(Options{MaxBufferBytes: 4096})
	chunk := `<tool_call>{"name":"search","arguments":{"q":"agentgate"}}</tool_call>`
	for i := 0; i < b.N; i++ {
		p.Reset()
		_ = p.Feed(chunk)
	}
}
