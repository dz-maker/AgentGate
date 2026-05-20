package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/agentgate/agentgate/internal/api/protocol"
	"github.com/agentgate/agentgate/internal/backend"
	"github.com/agentgate/agentgate/internal/cache/prefix"
	"github.com/agentgate/agentgate/internal/cache/semantic"
	"github.com/agentgate/agentgate/internal/capability"
	"github.com/agentgate/agentgate/internal/fallback"
	agenttrace "github.com/agentgate/agentgate/internal/observe/trace"
	"github.com/agentgate/agentgate/internal/parser"
	"github.com/agentgate/agentgate/internal/policy"
	"github.com/agentgate/agentgate/internal/router"
	"github.com/agentgate/agentgate/pkg/types"
)

const (
	defaultToolEarlyStopSavedEstimate = 64
	maxToolEarlyStopSavedEstimate     = 64
)

type Server struct {
	router             *router.Router
	registry           *backend.Registry
	prefix             *prefix.Service
	semantic           *semantic.Service
	policy             *policy.Engine
	breakers           *fallback.Set
	caps               *capability.Registry
	cost               *router.CostModel
	traces             *agenttrace.Store
	replay             *agenttrace.Replay
	enableToolParse    bool
	parserBuffer       int
	parserAggressive   bool
	logger             *slog.Logger
}

type Options struct {
	Router             *router.Router
	Registry           *backend.Registry
	Prefix             *prefix.Service
	Semantic           *semantic.Service
	Policy             *policy.Engine
	Breakers           *fallback.Set
	Caps               *capability.Registry
	Cost               *router.CostModel
	TraceStore         *agenttrace.Store
	Replay             *agenttrace.Replay
	EnableToolParse    bool
	ParserBuffer       int
	ParserAggressive   bool
	Logger             *slog.Logger
}

type requestAttempt struct {
	req      types.Request
	decision router.Decision
	policy   policy.Decision
	cache    semantic.AccessOptions
}

type executionResult struct {
	resp           *types.Response
	attempt        requestAttempt
	outcomes       []fallback.AttemptOutcome
	backendCharged bool
	cacheTier      string
	status         int
	err            error
}

type streamOpenResult struct {
	stream   <-chan types.Chunk
	attempt  requestAttempt
	outcomes []fallback.AttemptOutcome
	status   int
	err      error
}

func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.TraceStore == nil {
		opts.TraceStore = agenttrace.NewStore("")
	}
	return &Server{
		router:           opts.Router,
		registry:         opts.Registry,
		prefix:           opts.Prefix,
		semantic:         opts.Semantic,
		policy:           opts.Policy,
		breakers:         opts.Breakers,
		caps:             opts.Caps,
		cost:             opts.Cost,
		traces:           opts.TraceStore,
		replay:           opts.Replay,
		enableToolParse:  opts.EnableToolParse,
		parserBuffer:     opts.ParserBuffer,
		parserAggressive: opts.ParserAggressive,
		logger:           opts.Logger,
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /v1/models", s.models)
	mux.HandleFunc("POST /v1/chat/completions", s.chatCompletions)
	mux.HandleFunc("GET /admin/backends", s.backendStats)
	mux.HandleFunc("GET /admin/capabilities", s.capabilities)
	mux.HandleFunc("GET /admin/prefix/stats", s.prefixStats)
	mux.HandleFunc("GET /admin/prefix/topk", s.prefixTopK)
	mux.HandleFunc("GET /admin/cache/stats", s.cacheStats)
	mux.HandleFunc("GET /admin/breakers", s.breakerStats)
	mux.HandleFunc("GET /admin/cost", s.costStats)
	mux.HandleFunc("GET /admin/policy/budgets", s.policyBudgets)
	mux.HandleFunc("GET /debug/trace/{trace_id}", s.debugTrace)
	mux.HandleFunc("GET /debug/trace/{trace_id}/replay", s.debugReplay)
	return withCORS(mux)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"time":   time.Now().UTC(),
	})
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		OwnedBy string `json:"owned_by"`
	}
	seen := map[string]bool{}
	var data []model
	for _, b := range s.registry.All() {
		for _, id := range b.Capabilities().SupportedModels {
			if seen[id] {
				continue
			}
			seen[id] = true
			data = append(data, model{ID: id, Object: "model", OwnedBy: b.Name()})
		}
	}
	if len(data) == 0 {
		data = append(data, model{ID: "agentgate-default", Object: "model", OwnedBy: "agentgate"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *Server) chatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, 32*1024*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var openaiReq protocol.ChatCompletionRequest
	if err := json.Unmarshal(raw, &openaiReq); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	req, err := openaiReq.Normalize(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ensureTraceFields(&req)
	w.Header().Set("X-AgentGate-Trace-Id", req.TraceID)
	w.Header().Set("X-AgentGate-Step-Id", req.StepID)

	span := agenttrace.Span{
		TraceID:      req.TraceID,
		SessionID:    req.SessionID,
		AgentID:      req.AgentID,
		StepID:       req.StepID,
		ParentStepID: req.ParentStepID,
		StepType:     req.StepType,
		StartedAt:    time.Now(),
		TenantID:     req.TenantID,
		Model:        req.Model,
		Status:       "success",
	}
	defer func() {
		span.FinishedAt = time.Now()
		span.LatencyMs = span.FinishedAt.Sub(span.StartedAt).Milliseconds()
		if err := s.traces.Write(span); err != nil {
			s.logger.Warn("write trace span", "err", err, "trace_id", span.TraceID)
		}
		s.logger.Info("agent trace span",
			"trace_id", span.TraceID,
			"step_id", span.StepID,
			"tenant", span.TenantID,
			"model", span.Model,
			"backend", span.Backend,
			"instance", span.Instance,
			"status", span.Status,
			"prefix_match_tokens", span.PrefixMatchTokens,
			"latency_ms", span.LatencyMs,
		)
	}()

	if req.Model == "" {
		span.Status = "error"
		span.ErrorMessage = "model is required"
		writeError(w, http.StatusBadRequest, "model is required")
		return
	}

	routePolicy := s.policyRoutingDecision(req)
	if req.Stream {
		s.streamChat(w, r, req, routePolicy, &span)
		return
	}

	result := s.completeWithControl(r.Context(), req, routePolicy, &span)
	if result.err != nil {
		span.Status = "error"
		span.ErrorMessage = result.err.Error()
		if errors.Is(result.err, backend.ErrNoHealthyBackend) {
			result.status = http.StatusServiceUnavailable
		}
		if result.status == 0 {
			result.status = http.StatusBadGateway
		}
		setBudgetRetryAfter(w, result.attempt.policy.BudgetRetryAfter)
		writeError(w, result.status, result.err.Error())
		return
	}

	s.applySuccess(w, req, result, &span)
	writeJSON(w, http.StatusOK, result.resp)
}

func computeUSD(p types.CostProfile, u types.Usage) float64 {
	return p.InputUSDPer1K*float64(u.PromptTokens)/1000.0 +
		p.OutputUSDPer1K*float64(u.CompletionTokens)/1000.0
}

func (s *Server) policyRoutingDecision(req types.Request) policy.Decision {
	if s.policy == nil || s.policy.Empty() || s.registry == nil {
		return policy.Decision{}
	}
	if d := s.policy.Evaluate(req, ""); d.MatchedRoutingRule != "" {
		return d
	}
	for _, b := range s.registry.All() {
		d := s.policy.Evaluate(req, b.Capabilities().Vendor)
		if d.MatchedRoutingRule != "" {
			return d
		}
	}
	return policy.Decision{}
}

func (s *Server) policyForBackend(req types.Request, b backend.Backend) policy.Decision {
	if s.policy == nil || s.policy.Empty() || b == nil {
		return policy.Decision{}
	}
	return s.policy.Evaluate(req, b.Capabilities().Vendor)
}

func cacheOptionsFromPolicy(d policy.Decision) semantic.AccessOptions {
	opts := semantic.AccessOptions{
		Tier: d.CacheTier,
		TTL:  d.CacheTTL,
	}
	if d.CacheUse != nil {
		if *d.CacheUse {
			opts.ExplicitUse = true
		} else {
			opts.Skip = true
		}
	}
	return opts
}

func (s *Server) defaultAttempt(ctx context.Context, req types.Request) (requestAttempt, error) {
	if s.cost != nil && s.registry != nil {
		var best requestAttempt
		bestSet := false
		bestHasPrefix := false
		bestScore := 0.0
		for _, b := range s.registry.All() {
			if !b.Healthy() || !supportsModel(b.Capabilities(), req.Model) {
				continue
			}
			attempt, err := s.attemptForBackend(ctx, req, b)
			if err != nil {
				continue
			}
			hasPrefix := attempt.decision.PrefixMatch.MatchedTokens > 0
			score := s.cost.Score(b.Name(), b.Capabilities())
			if !bestSet ||
				(hasPrefix && !bestHasPrefix) ||
				(hasPrefix == bestHasPrefix && betterAttempt(attempt, score, best, bestScore, hasPrefix)) {
				best = attempt
				bestSet = true
				bestHasPrefix = hasPrefix
				bestScore = score
			}
		}
		if bestSet {
			return best, nil
		}
	}
	decision, err := s.router.Route(ctx, req)
	if err != nil {
		return requestAttempt{}, err
	}
	return s.attemptFromDecision(req, decision), nil
}

func betterAttempt(candidate requestAttempt, candidateScore float64, current requestAttempt, currentScore float64, comparePrefix bool) bool {
	if comparePrefix {
		if candidate.decision.PrefixMatch.MatchedTokens != current.decision.PrefixMatch.MatchedTokens {
			return candidate.decision.PrefixMatch.MatchedTokens > current.decision.PrefixMatch.MatchedTokens
		}
	}
	return candidateScore < currentScore
}

func supportsModel(caps types.Capabilities, model string) bool {
	if model == "" || len(caps.SupportedModels) == 0 {
		return true
	}
	for _, supported := range caps.SupportedModels {
		if supported == model {
			return true
		}
	}
	return false
}

func (s *Server) attemptForBackend(ctx context.Context, req types.Request, b backend.Backend) (requestAttempt, error) {
	decision, err := s.router.RouteBackend(ctx, req, b)
	if err != nil {
		return requestAttempt{}, err
	}
	return s.attemptFromDecision(req, decision), nil
}

func (s *Server) attemptFromDecision(req types.Request, decision router.Decision) requestAttempt {
	req.DesiredInstance = decision.InstanceID
	req.RoutedBackend = decision.Backend.Name()
	policyDecision := s.policyForBackend(req, decision.Backend)
	return requestAttempt{
		req:      req,
		decision: decision,
		policy:   policyDecision,
		cache:    cacheOptionsFromPolicy(policyDecision),
	}
}

func (s *Server) completeWithControl(ctx context.Context, req types.Request, routePolicy policy.Decision, span *agenttrace.Span) executionResult {
	if len(routePolicy.BackendChain) > 0 {
		return s.completeChain(ctx, req, routePolicy.BackendChain, span)
	}
	attempt, err := s.defaultAttempt(ctx, req)
	if err != nil {
		return executionResult{status: statusForBackendErr(err), err: err}
	}
	return s.completeAttempt(ctx, attempt)
}

func (s *Server) completeChain(ctx context.Context, req types.Request, names []string, span *agenttrace.Span) executionResult {
	var outcomes []fallback.AttemptOutcome
	var lastErr error
	for _, name := range names {
		b, ok := s.registry.ByName(name)
		if !ok {
			outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name, Skipped: true, SkipReason: "backend not registered"})
			continue
		}
		breaker := s.breakerFor(name)
		if breaker != nil && !breaker.Allow() {
			outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name, Skipped: true, SkipReason: "breaker open"})
			continue
		}
		attempt, err := s.attemptForBackend(ctx, req, b)
		if err != nil {
			if breaker != nil {
				breaker.Failure()
			}
			outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name, Err: err})
			lastErr = err
			continue
		}
		result := s.completeAttempt(ctx, attempt)
		if result.err != nil {
			if result.status == http.StatusTooManyRequests {
				result.outcomes = outcomes
				return result
			}
			if breaker != nil && result.backendCharged {
				breaker.Failure()
			}
			outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name, Err: result.err})
			lastErr = result.err
			continue
		}
		if breaker != nil && result.backendCharged {
			breaker.Success()
		}
		outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name})
		result.outcomes = outcomes
		if fallbackReason(outcomes) != "" && result.cacheTier == "" {
			span.FallbackReason = fallbackReason(outcomes)
		}
		return result
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("all %d backends in policy chain were unavailable", len(names))
	}
	return executionResult{outcomes: outcomes, status: statusForBackendErr(lastErr), err: lastErr}
}

func (s *Server) completeAttempt(ctx context.Context, attempt requestAttempt) executionResult {
	if attempt.policy.BudgetExceeded {
		return executionResult{
			attempt: attempt,
			status:  http.StatusTooManyRequests,
			err:     errors.New(attempt.policy.BudgetReason),
		}
	}
	if s.semantic != nil {
		if hit := s.semantic.LookupWithOptions(&attempt.req, attempt.cache); hit.Tier != "" {
			return executionResult{resp: hit.Response, attempt: attempt, cacheTier: hit.Tier}
		}
		if s.semantic.Cacheable(&attempt.req, attempt.cache) {
			key := semantic.ExactKey(&attempt.req)
			resp, err, originator := s.semantic.Singleflight().Do(key, func() (*types.Response, error) {
				return attempt.decision.Backend.Complete(ctx, &attempt.req)
			})
			if err != nil {
				return executionResult{attempt: attempt, backendCharged: originator, err: err}
			}
			if originator {
				s.semantic.StoreWithOptions(&attempt.req, resp, attempt.cache)
				return executionResult{resp: resp, attempt: attempt, backendCharged: true}
			}
			return executionResult{resp: resp, attempt: attempt, cacheTier: "singleflight"}
		}
	}
	resp, err := attempt.decision.Backend.Complete(ctx, &attempt.req)
	if err != nil {
		return executionResult{attempt: attempt, backendCharged: true, err: err}
	}
	return executionResult{resp: resp, attempt: attempt, backendCharged: true}
}

func (s *Server) breakerFor(name string) *fallback.Breaker {
	if s.breakers == nil {
		return nil
	}
	return s.breakers.For(name)
}

func fallbackReason(outcomes []fallback.AttemptOutcome) string {
	for _, outcome := range outcomes {
		if outcome.Skipped && outcome.SkipReason != "" {
			return outcome.BackendName + ": " + outcome.SkipReason
		}
		if outcome.Err != nil {
			return outcome.BackendName + ": " + outcome.Err.Error()
		}
	}
	return ""
}

func statusForBackendErr(err error) int {
	if errors.Is(err, backend.ErrNoHealthyBackend) {
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}

func (s *Server) applySuccess(w http.ResponseWriter, original types.Request, result executionResult, span *agenttrace.Span) {
	attempt := result.attempt
	decision := attempt.decision
	resp := result.resp
	span.Backend = decision.Backend.Name()
	span.Instance = decision.InstanceID
	span.PrefixMatchTokens = decision.PrefixMatch.MatchedTokens
	span.PrefixMatchReason = decision.PrefixMatch.Reason
	if resp != nil && resp.Usage != nil {
		span.PromptTokens = resp.Usage.PromptTokens
		span.CompletionTokens = resp.Usage.CompletionTokens
		span.TotalTokens = resp.Usage.TotalTokens
	}
	if result.cacheTier != "" {
		span.FallbackReason = "cache_" + result.cacheTier
		w.Header().Set("X-AgentGate-Cache", result.cacheTier)
	}
	setDecisionHeaders(w, attempt)
	if !result.backendCharged || resp == nil {
		return
	}
	s.router.Feedback(attempt.req, decision)
	if resp.Usage != nil {
		if s.cost != nil {
			s.cost.Observe(decision.Backend.Name(), resp.Usage.TotalTokens, time.Since(span.StartedAt))
		}
		if s.policy != nil && !s.policy.Empty() {
			usd := computeUSD(decision.Backend.Capabilities().CostProfile, *resp.Usage)
			s.policy.AccountUsage(original, decision.Backend.Capabilities().Vendor, resp.Usage.TotalTokens, usd)
		}
	}
}

func setDecisionHeaders(w http.ResponseWriter, attempt requestAttempt) {
	decision := attempt.decision
	if attempt.policy.MatchedRoutingRule != "" {
		w.Header().Set("X-AgentGate-Policy-Rule", attempt.policy.MatchedRoutingRule)
	}
	w.Header().Set("X-AgentGate-Backend", decision.Backend.Name())
	w.Header().Set("X-AgentGate-Instance", decision.InstanceID)
	w.Header().Set("X-AgentGate-Prefix-Matched-Tokens", strconv.Itoa(decision.PrefixMatch.MatchedTokens))
}

func (s *Server) openStreamWithControl(ctx context.Context, req types.Request, routePolicy policy.Decision) streamOpenResult {
	if len(routePolicy.BackendChain) > 0 {
		return s.openStreamChain(ctx, req, routePolicy.BackendChain)
	}
	attempt, err := s.defaultAttempt(ctx, req)
	if err != nil {
		return streamOpenResult{status: statusForBackendErr(err), err: err}
	}
	return s.openStreamAttempt(ctx, attempt)
}

func (s *Server) openStreamChain(ctx context.Context, req types.Request, names []string) streamOpenResult {
	var outcomes []fallback.AttemptOutcome
	var lastErr error
	for _, name := range names {
		b, ok := s.registry.ByName(name)
		if !ok {
			outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name, Skipped: true, SkipReason: "backend not registered"})
			continue
		}
		breaker := s.breakerFor(name)
		if breaker != nil && !breaker.Allow() {
			outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name, Skipped: true, SkipReason: "breaker open"})
			continue
		}
		attempt, err := s.attemptForBackend(ctx, req, b)
		if err != nil {
			if breaker != nil {
				breaker.Failure()
			}
			outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name, Err: err})
			lastErr = err
			continue
		}
		result := s.openStreamAttempt(ctx, attempt)
		if result.err != nil {
			if result.status == http.StatusTooManyRequests {
				result.outcomes = outcomes
				return result
			}
			if breaker != nil {
				breaker.Failure()
			}
			outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name, Err: result.err})
			lastErr = result.err
			continue
		}
		if breaker != nil {
			breaker.Success()
		}
		outcomes = append(outcomes, fallback.AttemptOutcome{BackendName: name})
		result.outcomes = outcomes
		return result
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("all %d backends in policy chain were unavailable", len(names))
	}
	return streamOpenResult{outcomes: outcomes, status: statusForBackendErr(lastErr), err: lastErr}
}

func (s *Server) openStreamAttempt(ctx context.Context, attempt requestAttempt) streamOpenResult {
	if attempt.policy.BudgetExceeded {
		return streamOpenResult{
			attempt: attempt,
			status:  http.StatusTooManyRequests,
			err:     errors.New(attempt.policy.BudgetReason),
		}
	}
	stream, err := attempt.decision.Backend.Stream(ctx, &attempt.req)
	if err != nil {
		return streamOpenResult{attempt: attempt, status: statusForBackendErr(err), err: err}
	}
	return streamOpenResult{stream: stream, attempt: attempt}
}

func (s *Server) streamChat(w http.ResponseWriter, r *http.Request, req types.Request, routePolicy policy.Decision, span *agenttrace.Span) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	opened := s.openStreamWithControl(ctx, req, routePolicy)
	if opened.err != nil {
		span.Status = "error"
		span.ErrorMessage = opened.err.Error()
		setBudgetRetryAfter(w, opened.attempt.policy.BudgetRetryAfter)
		if opened.status == 0 {
			opened.status = http.StatusBadGateway
		}
		writeError(w, opened.status, opened.err.Error())
		return
	}
	attempt := opened.attempt
	req = attempt.req
	decision := attempt.decision
	stream := opened.stream
	span.Backend = decision.Backend.Name()
	span.Instance = decision.InstanceID
	span.PrefixMatchTokens = decision.PrefixMatch.MatchedTokens
	span.PrefixMatchReason = decision.PrefixMatch.Reason
	if reason := fallbackReason(opened.outcomes); reason != "" {
		span.FallbackReason = reason
	}
	setDecisionHeaders(w, attempt)

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	send := func(v any) bool {
		data, err := json.Marshal(v)
		if err != nil {
			s.logger.Warn("marshal stream chunk", "err", err)
			return false
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	done := func() {
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		flusher.Flush()
	}

	var toolParser *parser.StreamingParser
	decodedTokensApprox := 0
	if s.enableToolParse && len(req.Tools) > 0 {
		toolParser = parser.NewStreamingParser(parser.Options{
			MaxBufferBytes:  s.parserBuffer,
			AggressiveAbort: s.parserAggressive,
		})
	}

	completed := false
	for chunk := range stream {
		if len(chunk.ToolCalls) > 0 || chunk.FinishReason != "" {
			if toolParser != nil && len(chunk.ToolCalls) > 0 {
				toolParser.Reset()
			}
			if chunk.Usage != nil {
				span.PromptTokens = chunk.Usage.PromptTokens
				span.CompletionTokens = chunk.Usage.CompletionTokens
				span.TotalTokens = chunk.Usage.TotalTokens
			}
			if !send(protocol.ChunkFromBackend(chunk)) {
				span.Status = "partial"
				span.ErrorMessage = "client disconnected while sending stream chunk"
				return
			}
			if chunk.FinishReason != "" {
				completed = true
				break
			}
			continue
		}

		if chunk.Content == "" {
			continue
		}
		decodedTokensApprox += estimateTokensFromText(chunk.Content)

		if toolParser == nil {
			if !send(protocol.ChunkFromBackend(chunk)) {
				return
			}
			continue
		}

		ev := toolParser.Feed(chunk.Content)
		if ev.Text != "" {
			textChunk := chunk
			textChunk.Content = ev.Text
			if !send(protocol.ChunkFromBackend(textChunk)) {
				return
			}
		}
		if ev.ToolCall != nil {
			span.EarlyStopFired = true
			span.DecodeTokensSaved = estimateDecodeTokensSaved(req.MaxTokens, decodedTokensApprox)
			span.DecodeTokensSavedEstimated = true
			span.DecodeTokensEstimateMethod = "remaining_decode_budget_capped_at_64_tokens"
			if !send(protocol.ToolCallChunk(chunk.ID, req.Model, *ev.ToolCall)) {
				span.Status = "partial"
				span.ErrorMessage = "client disconnected while sending tool call"
				return
			}
			_ = send(protocol.FinishChunk(chunk.ID, req.Model, "tool_calls"))
			cancel()
			completed = true
			break
		}
		if ev.Kind == parser.EventAbort && ev.ShouldStop {
			// aggressive_abort: a tool call started but failed to parse
			// (or overflowed the buffer). Cut the upstream stream rather
			// than let the model continue speculating on output the
			// gateway has already partially echoed.
			_ = send(protocol.FinishChunk(chunk.ID, req.Model, "stop"))
			cancel()
			completed = true
			break
		}
	}

	if !completed && toolParser != nil {
		if tail := toolParser.Flush(); tail != "" {
			_ = send(protocol.ChunkFromBackend(types.Chunk{
				ID:      "chatcmpl_agentgate_tail",
				Model:   req.Model,
				Content: tail,
			}))
		}
	}

	s.router.Feedback(req, decision)
	if span.TotalTokens > 0 {
		if s.cost != nil {
			s.cost.Observe(decision.Backend.Name(), span.TotalTokens, time.Since(span.StartedAt))
		}
		if s.policy != nil && !s.policy.Empty() {
			usage := types.Usage{
				PromptTokens:     span.PromptTokens,
				CompletionTokens: span.CompletionTokens,
				TotalTokens:      span.TotalTokens,
			}
			usd := computeUSD(decision.Backend.Capabilities().CostProfile, usage)
			s.policy.AccountUsage(req, decision.Backend.Capabilities().Vendor, span.TotalTokens, usd)
		}
	}
	done()
}

func ensureTraceFields(req *types.Request) {
	if req.TraceID == "" {
		req.TraceID = agenttrace.NewID("trace")
	}
	if req.StepID == "" {
		req.StepID = agenttrace.NewID("step")
	}
	if req.StepType == "" {
		req.StepType = "llm_call"
	}
}

func estimateDecodeTokensSaved(maxTokens *int, decodedTokensApprox int) int {
	if maxTokens == nil {
		return defaultToolEarlyStopSavedEstimate
	}
	remaining := *maxTokens - decodedTokensApprox
	if remaining <= 0 {
		return 0
	}
	if remaining > maxToolEarlyStopSavedEstimate {
		return maxToolEarlyStopSavedEstimate
	}
	return remaining
}

func estimateTokensFromText(s string) int {
	if s == "" {
		return 0
	}
	tokens := len([]rune(s)) / 4
	if tokens == 0 {
		return 1
	}
	return tokens
}

func (s *Server) backendStats(w http.ResponseWriter, r *http.Request) {
	var stats []types.BackendStats
	for _, b := range s.registry.All() {
		stats = append(stats, b.Stats())
	}
	writeJSON(w, http.StatusOK, map[string]any{"backends": stats})
}

func (s *Server) prefixStats(w http.ResponseWriter, r *http.Request) {
	if s.prefix == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	writeJSON(w, http.StatusOK, s.prefix.Stats(0))
}

func (s *Server) prefixTopK(w http.ResponseWriter, r *http.Request) {
	if s.prefix == nil {
		writeJSON(w, http.StatusOK, []prefix.TopKey{})
		return
	}
	n := 20
	if raw := r.URL.Query().Get("n"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 100 {
			n = parsed
		}
	}
	writeJSON(w, http.StatusOK, s.prefix.TopK(n))
}

func (s *Server) debugTrace(w http.ResponseWriter, r *http.Request) {
	traceID := r.PathValue("trace_id")
	if traceID == "" {
		writeError(w, http.StatusBadRequest, "trace_id is required")
		return
	}
	summary := s.traces.Get(traceID)
	if len(summary.Spans) == 0 {
		writeError(w, http.StatusNotFound, "trace not found")
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (s *Server) debugReplay(w http.ResponseWriter, r *http.Request) {
	if s.replay == nil {
		writeError(w, http.StatusServiceUnavailable, "replay not configured")
		return
	}
	traceID := r.PathValue("trace_id")
	if traceID == "" {
		writeError(w, http.StatusBadRequest, "trace_id is required")
		return
	}
	lookback := 7
	if raw := r.URL.Query().Get("lookback_days"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 && v <= 90 {
			lookback = v
		}
	}
	summary, err := s.replay.Lookup(traceID, lookback)
	if err != nil {
		if errors.Is(err, agenttrace.ErrTraceNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (s *Server) capabilities(w http.ResponseWriter, r *http.Request) {
	if s.caps == nil {
		writeJSON(w, http.StatusOK, map[string]any{"sheets": []any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sheets": s.caps.All()})
}

func (s *Server) cacheStats(w http.ResponseWriter, r *http.Request) {
	if s.semantic == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	writeJSON(w, http.StatusOK, s.semantic.Stats())
}

func (s *Server) breakerStats(w http.ResponseWriter, r *http.Request) {
	if s.breakers == nil {
		writeJSON(w, http.StatusOK, map[string]any{"breakers": map[string]any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"breakers": s.breakers.Snapshot()})
}

func (s *Server) costStats(w http.ResponseWriter, r *http.Request) {
	if s.cost == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	writeJSON(w, http.StatusOK, s.cost.Snapshot())
}

func (s *Server) policyBudgets(w http.ResponseWriter, r *http.Request) {
	if s.policy == nil {
		writeJSON(w, http.StatusOK, map[string]any{"budgets": map[string]any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"budgets": s.policy.SnapshotBudgets()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"error":{"message":"failed to encode JSON response","type":"agentgate_error"}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(data, '\n'))
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    "agentgate_error",
		},
	})
}

func setBudgetRetryAfter(w http.ResponseWriter, d time.Duration) {
	if d <= 0 {
		return
	}
	sec := int(d.Seconds())
	if sec < 1 {
		sec = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(sec))
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
