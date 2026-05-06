package parser

import "encoding/json"

type IncrementalJSON struct {
	depth     int
	inString  bool
	escaped   bool
	started   bool
	completed bool
	partial   []rune
}

func (p *IncrementalJSON) Feed(s string) (json.RawMessage, bool) {
	if p.completed {
		return json.RawMessage(string(p.partial)), true
	}

	for _, c := range s {
		if !p.started {
			if c != '{' {
				continue
			}
			p.started = true
		}

		p.partial = append(p.partial, c)

		switch {
		case p.escaped:
			p.escaped = false
		case c == '\\' && p.inString:
			p.escaped = true
		case c == '"':
			p.inString = !p.inString
		case !p.inString && c == '{':
			p.depth++
		case !p.inString && c == '}':
			p.depth--
			if p.depth == 0 {
				p.completed = true
				return json.RawMessage(string(p.partial)), true
			}
		}
	}

	return nil, false
}

func (p *IncrementalJSON) Reset() {
	*p = IncrementalJSON{}
}
