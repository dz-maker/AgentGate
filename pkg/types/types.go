package types

import (
	"encoding/json"
	"time"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type Message struct {
	Role       Role            `json:"role"`
	Content    any             `json:"content,omitempty"`
	Name       string          `json:"name,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCallDelta `json:"tool_calls,omitempty"`
}

func (m Message) ContentString() string {
	switch v := m.Content.(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

type ToolDefinition struct {
	Type     string          `json:"type"`
	Function json.RawMessage `json:"function,omitempty"`
	Raw      json.RawMessage `json:"-"`
}

type CachePolicy struct {
	PrefixHint  string   `json:"prefix_hint,omitempty" yaml:"prefix_hint,omitempty"`
	PinSegments []string `json:"pin_segments,omitempty" yaml:"pin_segments,omitempty"`
}

type AgentGateOptions struct {
	SessionID    string      `json:"session_id,omitempty"`
	TenantID     string      `json:"tenant_id,omitempty"`
	AgentID      string      `json:"agent_id,omitempty"`
	TraceID      string      `json:"trace_id,omitempty"`
	StepID       string      `json:"step_id,omitempty"`
	ParentStepID string      `json:"parent_step_id,omitempty"`
	StepType     string      `json:"step_type,omitempty"`
	PrefixHash   string      `json:"prefix_hash,omitempty"`
	CacheControl CachePolicy `json:"cache_control,omitempty"`
}

type Request struct {
	Model       string           `json:"model"`
	Messages    []Message        `json:"messages"`
	Tools       []ToolDefinition `json:"tools,omitempty"`
	Temperature *float64         `json:"temperature,omitempty"`
	TopP        *float64         `json:"top_p,omitempty"`
	MaxTokens   *int             `json:"max_tokens,omitempty"`
	Stop        []string         `json:"stop,omitempty"`
	Stream      bool             `json:"stream,omitempty"`

	ResponseFormat json.RawMessage `json:"response_format,omitempty"`
	ToolChoice     any             `json:"tool_choice,omitempty"`

	SessionID    string          `json:"-"`
	TenantID     string          `json:"-"`
	AgentID      string          `json:"-"`
	TraceID      string          `json:"-"`
	StepID       string          `json:"-"`
	ParentStepID string          `json:"-"`
	StepType     string          `json:"-"`
	PrefixHash   string          `json:"-"`
	CacheControl CachePolicy     `json:"-"`
	Raw          json.RawMessage `json:"-"`

	DesiredInstance string `json:"-"`
	RoutedBackend   string `json:"-"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens,omitempty"`
	CompletionTokens int `json:"completion_tokens,omitempty"`
	TotalTokens      int `json:"total_tokens,omitempty"`
}

type Response struct {
	ID      string   `json:"id,omitempty"`
	Object  string   `json:"object,omitempty"`
	Created int64    `json:"created,omitempty"`
	Model   string   `json:"model,omitempty"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message,omitempty"`
	Delta        Delta   `json:"delta,omitempty"`
	FinishReason string  `json:"finish_reason,omitempty"`
}

type Delta struct {
	Role      Role            `json:"role,omitempty"`
	Content   string          `json:"content,omitempty"`
	ToolCalls []ToolCallDelta `json:"tool_calls,omitempty"`
}

type ToolCallDelta struct {
	Index    int              `json:"index,omitempty"`
	ID       string           `json:"id,omitempty"`
	Type     string           `json:"type,omitempty"`
	Function ToolCallFunction `json:"function,omitempty"`
}

type ToolCall struct {
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type ToolCallFunction struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

type Chunk struct {
	ID           string
	Model        string
	Content      string
	ToolCalls    []ToolCallDelta
	FinishReason string
	Usage        *Usage
	Raw          json.RawMessage
	CreatedAt    time.Time
}

type BackendStats struct {
	Name             string          `json:"name"`
	Healthy          bool            `json:"healthy"`
	Instances        []InstanceStats `json:"instances,omitempty"`
	InFlight         int64           `json:"in_flight"`
	TotalRequests    uint64          `json:"total_requests"`
	FailedRequests   uint64          `json:"failed_requests"`
	LastError        string          `json:"last_error,omitempty"`
	PrefixCacheAware bool            `json:"prefix_cache_aware"`
}

type InstanceStats struct {
	ID              string    `json:"id"`
	Endpoint        string    `json:"endpoint"`
	Healthy         bool      `json:"healthy"`
	InFlight        int64     `json:"in_flight"`
	TotalRequests   uint64    `json:"total_requests"`
	FailedRequests  uint64    `json:"failed_requests"`
	LastSeen        time.Time `json:"last_seen,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
	PrefixHitHints  uint64    `json:"prefix_hit_hints"`
	PrefixMissHints uint64    `json:"prefix_miss_hints"`
}

// PrefixCacheMode describes how a backend caches prefill state.
//
// Different backends do prefix caching very differently and the upper-layer
// router has to know — sticky-routing a request to a backend with mode "none"
// is a waste, while mode "external_kv" implies a separate KV provider that
// holds the cache off-engine.
type PrefixCacheMode string

const (
	PrefixCacheNone       PrefixCacheMode = "none"
	PrefixCacheAPC        PrefixCacheMode = "apc"         // vLLM block-hash style
	PrefixCacheRadix      PrefixCacheMode = "radix"       // SGLang RadixAttention
	PrefixCacheExternalKV PrefixCacheMode = "external_kv" // LMCache / Dynamo
)

// CostProfile is what a single 1k-token slice costs at this backend. Used by
// cost-aware routing to break ties between equally-warm instances.
//
// All fields are USD per 1k tokens. Zero means "free" (e.g. self-hosted vLLM
// where the marginal cost per token is nearly zero compared to cloud APIs).
type CostProfile struct {
	InputUSDPer1K   float64 `json:"input_usd_per_1k,omitempty"`
	OutputUSDPer1K  float64 `json:"output_usd_per_1k,omitempty"`
	CachedInputDisc float64 `json:"cached_input_discount,omitempty"` // 0..1, e.g. 0.9 = 90% off
}

// Capabilities is the per-backend capability sheet that drives routing,
// caching, fallback and degradation decisions. Adapters fill it in once at
// startup (optionally refreshed by health probes) so upper-layer pipelines do
// not have to special-case backends by name.
type Capabilities struct {
	// Coarse feature flags, kept for backwards compatibility with v0.1.
	SupportsPrefixCache      bool `json:"supports_prefix_cache"`
	SupportsStructuredOutput bool `json:"supports_structured_output"`
	SupportsLogprobs         bool `json:"supports_logprobs"`
	SupportsStreaming        bool `json:"supports_streaming"`
	SupportsToolCalling      bool `json:"supports_tool_calling"`
	SupportsAbort            bool `json:"supports_abort"`

	// PrefixCacheMode is how the backend internally caches prefill state.
	// Empty/none means the backend is opaque to prefix sticky routing.
	PrefixCacheMode PrefixCacheMode `json:"prefix_cache_mode,omitempty"`
	KVProvider      string          `json:"kv_provider,omitempty"`

	MaxContextLength int      `json:"max_context_length,omitempty"`
	SupportedModels  []string `json:"supported_models,omitempty"`

	// CostProfile is optional. Cloud adapters fill this in; self-hosted
	// adapters (vLLM, SGLang, Ollama) typically leave it zero.
	CostProfile CostProfile `json:"cost_profile,omitempty"`

	// Vendor identifies the backend family ("vllm", "sglang", "ollama",
	// "openai", "anthropic", "mock"). Used in metrics and policy DSL.
	Vendor string `json:"vendor,omitempty"`
}
