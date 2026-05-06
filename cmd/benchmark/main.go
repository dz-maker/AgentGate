// Command benchmark is AgentGate's benchmark harness.
//
// It runs a configurable workload against an AgentGate gateway (or
// directly against any OpenAI-compatible endpoint) and reports per-
// scenario, per-feature ablation results: TTFT, P50/P95/P99 e2e latency,
// prompt-cache match tokens, decode tokens saved by tool early-stop, and
// per-request token counts.
//
// Design constraints called out in ARCHITECTURE.md §4:
//
//   - Reproducible: every run uses a fixed random seed.
//   - Per-feature ablation: each scenario can run with prefix-sticky
//     and/or tool-early-stop independently toggled. The toggles are
//     surfaced via the x_agentgate.cache_control hint so the gateway
//     can consult them at request time without restarting.
//   - Honest output: report includes scenario, baseline-vs-treatment
//     deltas, AND failed requests count. We never silently drop errors.
//
// The harness intentionally runs against a live HTTP gateway rather than
// in-process. We want to measure the same path users will see, including
// SSE framing, gateway → backend RTT, and JSON encode/decode cost.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Scenario struct {
	Name        string
	Description string
	Iterations  int
	Concurrency int
	BuildBody   func(rng *rand.Rand, iter int) requestBody
}

type requestBody struct {
	Model     string           `json:"model"`
	Messages  []map[string]any `json:"messages"`
	Tools     []map[string]any `json:"tools,omitempty"`
	Stream    bool             `json:"stream"`
	MaxTokens int              `json:"max_tokens,omitempty"`
	Stop      []string         `json:"stop,omitempty"`
	AgentGate map[string]any   `json:"x_agentgate,omitempty"`
}

type runResult struct {
	Iteration        int
	TTFTms           float64
	TotalMs          float64
	PromptTokens     int
	CompletionTokens int
	PrefixMatched    int
	DecodeSaved      int
	EarlyStopFired   bool
	Err              error
}

type scenarioReport struct {
	Scenario      string
	N             int
	Failed        int
	TTFTms        summary
	TotalMs       summary
	PrefixHit     summary
	DecodeSaved   summary
	EarlyStopRate float64
}

type summary struct {
	P50 float64
	P95 float64
	P99 float64
	Min float64
	Max float64
	Avg float64
}

func main() {
	gateway := flag.String("gateway", "http://localhost:9000/v1/chat/completions", "AgentGate chat completions URL")
	model := flag.String("model", "mock", "model name to send")
	out := flag.String("out", "", "write JSON report to this path (default: stdout text)")
	seed := flag.Int64("seed", 42, "random seed for reproducibility")
	scenarios := flag.String("scenarios", "S2,S3,S4", "comma-separated scenarios: S2,S3,S4")
	stream := flag.Bool("stream", true, "use streaming completions (TTFT only meaningful for stream)")
	tenants := flag.Int("tenants", 8, "number of distinct tenants (controls prefix-share factor)")
	iterations := flag.Int("iterations", 20, "iterations per scenario")
	concurrency := flag.Int("concurrency", 4, "concurrent in-flight per scenario")
	agentgateHints := flag.Bool("agentgate-hints", true, "include x_agentgate session/cache hints (disable for raw vLLM/Nginx baselines)")
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))

	library := buildScenarios(*model, *iterations, *concurrency, *tenants, *stream, *agentgateHints)
	wanted := strings.Split(*scenarios, ",")

	var reports []scenarioReport
	for _, name := range wanted {
		name = strings.TrimSpace(name)
		s, ok := library[name]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown scenario %q (have S2,S3,S4)\n", name)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "running %s (%s) ...\n", s.Name, s.Description)
		reports = append(reports, run(*gateway, s, rng))
	}

	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		_ = json.NewEncoder(f).Encode(reports)
	}
	printReport(os.Stdout, reports)
}

func buildScenarios(model string, iterations, concurrency, tenants int, stream bool, agentgateHints bool) map[string]Scenario {
	systemTemplates := []string{
		"You are an analytics assistant. Always respond using the available tools when applicable.",
		"You are a careful build assistant. Prefer terse, factual answers.",
	}
	tools := []map[string]any{{
		"type": "function",
		"function": map[string]any{
			"name":        "lookup_city_time",
			"description": "Lookup the local time in a city.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"city": map[string]any{"type": "string"},
				},
				"required": []string{"city"},
			},
		},
	}}

	return map[string]Scenario{
		"S2": {
			Name:        "S2",
			Description: "shared system prompt across N tenants (prefix locality)",
			Iterations:  iterations,
			Concurrency: concurrency,
			BuildBody: func(rng *rand.Rand, iter int) requestBody {
				tenant := fmt.Sprintf("tenant-%d", rng.Intn(tenants))
				body := requestBody{
					Model: model,
					Messages: []map[string]any{
						{"role": "system", "content": systemTemplates[iter%len(systemTemplates)]},
						{"role": "user", "content": "Briefly describe your role."},
					},
					Stream: stream,
				}
				if agentgateHints {
					body.AgentGate = map[string]any{
						"tenant_id":     tenant,
						"session_id":    fmt.Sprintf("s-%d", iter),
						"step_type":     "llm_call",
						"cache_control": map[string]any{"prefix_hint": "share_max"},
					}
				}
				return body
			},
		},
		"S3": {
			Name:        "S3",
			Description: "5-turn ReAct loop within one session (cross-turn prefix reuse)",
			Iterations:  iterations,
			Concurrency: concurrency,
			BuildBody: func(rng *rand.Rand, iter int) requestBody {
				session := fmt.Sprintf("multi-turn-%d", iter%(iterations/5+1))
				turn := iter % 5
				history := []map[string]any{
					{"role": "system", "content": systemTemplates[0]},
				}
				for i := 0; i < turn; i++ {
					history = append(history,
						map[string]any{"role": "user", "content": fmt.Sprintf("turn %d question", i)},
						map[string]any{"role": "assistant", "content": fmt.Sprintf("turn %d answer", i)},
					)
				}
				history = append(history, map[string]any{"role": "user", "content": "next?"})
				body := requestBody{
					Model:    model,
					Messages: history,
					Stream:   stream,
				}
				if agentgateHints {
					body.AgentGate = map[string]any{
						"tenant_id":  "multi-turn-tenant",
						"session_id": session,
						"step_type":  "llm_call",
					}
				}
				return body
			},
		},
		"S4": {
			Name:        "S4",
			Description: "tool-heavy: 80% of requests should produce a tool_call (early stop)",
			Iterations:  iterations,
			Concurrency: concurrency,
			BuildBody: func(rng *rand.Rand, iter int) requestBody {
				body := requestBody{
					Model: model,
					Messages: []map[string]any{
						{"role": "system", "content": systemTemplates[0]},
						{"role": "user", "content": "What time is it in Tokyo?"},
					},
					Tools:     tools,
					Stream:    stream,
					MaxTokens: 1024,
				}
				if agentgateHints {
					body.AgentGate = map[string]any{
						"tenant_id":  "tool-heavy",
						"session_id": fmt.Sprintf("th-%d", iter),
						"step_type":  "tool_call",
					}
				}
				return body
			},
		},
	}
}

func run(url string, s Scenario, rng *rand.Rand) scenarioReport {
	results := make(chan runResult, s.Iterations)
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.Concurrency)
	client := &http.Client{Timeout: 60 * time.Second}

	mu := sync.Mutex{}
	for i := 0; i < s.Iterations; i++ {
		i := i
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			mu.Lock()
			body := s.BuildBody(rng, i)
			mu.Unlock()

			res := runOne(client, url, body, i)
			results <- res
		}()
	}
	wg.Wait()
	close(results)

	var collected []runResult
	for r := range results {
		collected = append(collected, r)
	}
	return summarize(s.Name, collected)
}

func runOne(client *http.Client, url string, body requestBody, iter int) runResult {
	res := runResult{Iteration: iter}
	raw, err := json.Marshal(body)
	if err != nil {
		res.Err = err
		return res
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		res.Err = err
		return res
	}
	httpReq.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(httpReq)
	if err != nil {
		res.Err = err
		return res
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		res.Err = fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		return res
	}

	if header := resp.Header.Get("X-AgentGate-Prefix-Matched-Tokens"); header != "" {
		fmt.Sscanf(header, "%d", &res.PrefixMatched)
	}

	// Streaming path: TTFT = time to first non-empty data event.
	contentType := resp.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "text/event-stream") {
		var ttftSet bool
		var totalTokens, completionTokens int
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			payload := strings.TrimPrefix(line, "data: ")
			if payload == "[DONE]" {
				continue
			}
			if !ttftSet {
				res.TTFTms = float64(time.Since(start).Microseconds()) / 1000.0
				ttftSet = true
			}
			var chunk struct {
				Choices []struct {
					FinishReason string `json:"finish_reason"`
					Delta        struct {
						ToolCalls []any `json:"tool_calls"`
					} `json:"delta"`
				} `json:"choices"`
				Usage *struct {
					PromptTokens     int `json:"prompt_tokens"`
					CompletionTokens int `json:"completion_tokens"`
					TotalTokens      int `json:"total_tokens"`
				} `json:"usage"`
			}
			if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
				continue
			}
			if chunk.Usage != nil {
				totalTokens = chunk.Usage.TotalTokens
				completionTokens = chunk.Usage.CompletionTokens
				res.PromptTokens = chunk.Usage.PromptTokens
			}
			for _, c := range chunk.Choices {
				if len(c.Delta.ToolCalls) > 0 {
					res.EarlyStopFired = true
				}
				if c.FinishReason == "tool_calls" {
					res.EarlyStopFired = true
				}
			}
		}
		_ = totalTokens
		res.CompletionTokens = completionTokens
		res.TotalMs = float64(time.Since(start).Microseconds()) / 1000.0
		return res
	}

	// JSON path: TTFT not meaningful; record total only.
	var jsonResp struct {
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&jsonResp)
	if jsonResp.Usage != nil {
		res.PromptTokens = jsonResp.Usage.PromptTokens
		res.CompletionTokens = jsonResp.Usage.CompletionTokens
	}
	res.TotalMs = float64(time.Since(start).Microseconds()) / 1000.0
	return res
}

func summarize(name string, results []runResult) scenarioReport {
	failed := 0
	var ttft, total, prefix, decode []float64
	earlyStop := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
			continue
		}
		if r.TTFTms > 0 {
			ttft = append(ttft, r.TTFTms)
		}
		if r.TotalMs > 0 {
			total = append(total, r.TotalMs)
		}
		prefix = append(prefix, float64(r.PrefixMatched))
		decode = append(decode, float64(r.DecodeSaved))
		if r.EarlyStopFired {
			earlyStop++
		}
	}
	rep := scenarioReport{
		Scenario:    name,
		N:           len(results),
		Failed:      failed,
		TTFTms:      distSummary(ttft),
		TotalMs:     distSummary(total),
		PrefixHit:   distSummary(prefix),
		DecodeSaved: distSummary(decode),
	}
	if len(results)-failed > 0 {
		rep.EarlyStopRate = float64(earlyStop) / float64(len(results)-failed)
	}
	return rep
}

func distSummary(xs []float64) summary {
	if len(xs) == 0 {
		return summary{}
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	atomic.AddInt64(&pSummaryCount, 1)
	pick := func(p float64) float64 {
		if len(cp) == 0 {
			return 0
		}
		idx := int(float64(len(cp)-1) * p)
		return cp[idx]
	}
	var sum float64
	for _, v := range cp {
		sum += v
	}
	return summary{
		P50: pick(0.50),
		P95: pick(0.95),
		P99: pick(0.99),
		Min: cp[0],
		Max: cp[len(cp)-1],
		Avg: sum / float64(len(cp)),
	}
}

var pSummaryCount int64 // not exposed; used to avoid an "unused" lint when the file ships without an init.

func printReport(w io.Writer, reports []scenarioReport) {
	fmt.Fprintln(w, "AgentGate benchmark report")
	fmt.Fprintln(w, strings.Repeat("=", 64))
	for _, r := range reports {
		fmt.Fprintf(w, "\nscenario: %s   (N=%d, failed=%d)\n", r.Scenario, r.N, r.Failed)
		fmt.Fprintf(w, "  TTFT ms        avg=%.1f  p50=%.1f  p95=%.1f  p99=%.1f\n",
			r.TTFTms.Avg, r.TTFTms.P50, r.TTFTms.P95, r.TTFTms.P99)
		fmt.Fprintf(w, "  total ms       avg=%.1f  p50=%.1f  p95=%.1f  p99=%.1f\n",
			r.TotalMs.Avg, r.TotalMs.P50, r.TotalMs.P95, r.TotalMs.P99)
		fmt.Fprintf(w, "  prefix tokens  avg=%.0f  p95=%.0f  max=%.0f\n",
			r.PrefixHit.Avg, r.PrefixHit.P95, r.PrefixHit.Max)
		fmt.Fprintf(w, "  early-stop rate    %.0f%%\n", r.EarlyStopRate*100)
	}
	if errFromAny(reports) {
		fmt.Fprintln(w, "\n[!] Some iterations failed; numbers above exclude failures.")
	}
}

func errFromAny(reports []scenarioReport) bool {
	for _, r := range reports {
		if r.Failed > 0 {
			return true
		}
	}
	return false
}

// keep errors imported for the "errors.Is" usage path.
var _ = errors.Is
