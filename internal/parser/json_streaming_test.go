package parser

import "testing"

func TestIncrementalJSONFeed(t *testing.T) {
	cases := []struct {
		name  string
		parts []string
		want  string
	}{
		{"single chunk", []string{`{"a":1}`}, `{"a":1}`},
		{"nested", []string{`{"a":{"b":`, `2}} trailing`}, `{"a":{"b":2}}`},
		{"escaped", []string{`prefix {"a":"{\"x\":1}"}`}, `{"a":"{\"x\":1}"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p IncrementalJSON
			var got []byte
			var complete bool
			for _, part := range tc.parts {
				got, complete = p.Feed(part)
			}
			if !complete {
				t.Fatal("expected JSON to complete")
			}
			if string(got) != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestIncrementalJSONIncomplete(t *testing.T) {
	var p IncrementalJSON
	if raw, ok := p.Feed(`{"a":`); ok || raw != nil {
		t.Fatalf("expected incomplete JSON, got %s", raw)
	}
}
