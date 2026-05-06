package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Backends   []BackendConfig  `yaml:"backends"`
	Prefix     PrefixConfig     `yaml:"prefix_cache"`
	Semantic   SemanticConfig   `yaml:"semantic_cache"`
	ToolParser ToolParserConfig `yaml:"tool_parser"`
	Timeouts   TimeoutConfig    `yaml:"timeouts"`
	Policy     PolicyConfig     `yaml:"policy"`
	Telemetry  TelemetryConfig  `yaml:"telemetry"`
	Fallback   FallbackConfig   `yaml:"fallback"`
	TraceDir   string           `yaml:"trace_dir"`
}

type PolicyConfig struct {
	Path string `yaml:"path"`
}

type TelemetryConfig struct {
	OTLP OTLPConfig `yaml:"otlp"`
}

type OTLPConfig struct {
	Endpoint    string            `yaml:"endpoint"`
	Headers     map[string]string `yaml:"headers"`
	ServiceName string            `yaml:"service_name"`
	BatchSize   int               `yaml:"batch_size"`
	FlushEvery  time.Duration     `yaml:"flush_every"`
}

type FallbackConfig struct {
	FailureThreshold int           `yaml:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold"`
	Cooldown         time.Duration `yaml:"cooldown"`
}

type SemanticConfig struct {
	Enabled    *bool         `yaml:"enabled"`
	MaxEntries int           `yaml:"max_entries"`
	TTLExact   time.Duration `yaml:"ttl_exact"`
	TTLTool    time.Duration `yaml:"ttl_tool"`
}

type ServerConfig struct {
	Addr string `yaml:"addr"`
}

type BackendConfig struct {
	Name      string            `yaml:"name"`
	Type      string            `yaml:"type"`
	Endpoint  string            `yaml:"endpoint"`
	Endpoints []string          `yaml:"endpoints"`
	Discovery DiscoveryConfig   `yaml:"discovery"`
	Headers   map[string]string `yaml:"headers"`
	APIKey    string            `yaml:"api_key,omitempty"`
	Vendor    string            `yaml:"vendor,omitempty"`
	Models    []string          `yaml:"models,omitempty"`
	Cost      CostProfile       `yaml:"cost,omitempty"`
}

type CostProfile struct {
	InputUSDPer1K   float64 `yaml:"input_usd_per_1k,omitempty"`
	OutputUSDPer1K  float64 `yaml:"output_usd_per_1k,omitempty"`
	CachedInputDisc float64 `yaml:"cached_input_discount,omitempty"`
}

type DiscoveryConfig struct {
	Type      string   `yaml:"type"`
	Endpoints []string `yaml:"endpoints"`
}

type PrefixConfig struct {
	Enabled      *bool         `yaml:"enabled"`
	MaxEntries   int           `yaml:"max_entries"`
	HalfLife     time.Duration `yaml:"half_life"`
	DebugContent bool          `yaml:"debug_content"`
}

type ToolParserConfig struct {
	Enabled         *bool `yaml:"enabled"`
	AggressiveAbort bool  `yaml:"aggressive_abort"`
	MaxBufferBytes  int   `yaml:"max_buffer_bytes"`
}

type TimeoutConfig struct {
	Request     time.Duration `yaml:"request"`
	Header      time.Duration `yaml:"header"`
	HealthCheck time.Duration `yaml:"health_check"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data = []byte(os.ExpandEnv(string(data)))

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Addr == "" {
		c.Server.Addr = ":9000"
	}
	if c.Prefix.Enabled == nil {
		v := true
		c.Prefix.Enabled = &v
	}
	if c.Prefix.MaxEntries == 0 {
		c.Prefix.MaxEntries = 100_000
	}
	if c.Prefix.HalfLife == 0 {
		c.Prefix.HalfLife = 5 * time.Minute
	}
	if c.ToolParser.MaxBufferBytes == 0 {
		c.ToolParser.MaxBufferBytes = 16 * 1024
	}
	if c.ToolParser.Enabled == nil {
		v := true
		c.ToolParser.Enabled = &v
	}
	if c.Timeouts.Header == 0 {
		c.Timeouts.Header = 30 * time.Second
	}
	if c.Timeouts.HealthCheck == 0 {
		c.Timeouts.HealthCheck = 2 * time.Second
	}
	if c.TraceDir == "" {
		c.TraceDir = "traces"
	}
	if c.Semantic.Enabled == nil {
		v := true
		c.Semantic.Enabled = &v
	}
	if c.Semantic.MaxEntries == 0 {
		c.Semantic.MaxEntries = 10_000
	}
	if c.Semantic.TTLExact == 0 {
		c.Semantic.TTLExact = 5 * time.Minute
	}
	if c.Semantic.TTLTool == 0 {
		c.Semantic.TTLTool = 10 * time.Minute
	}
	if c.Fallback.FailureThreshold == 0 {
		c.Fallback.FailureThreshold = 5
	}
	if c.Fallback.SuccessThreshold == 0 {
		c.Fallback.SuccessThreshold = 2
	}
	if c.Fallback.Cooldown == 0 {
		c.Fallback.Cooldown = 10 * time.Second
	}
	if c.Telemetry.OTLP.BatchSize == 0 {
		c.Telemetry.OTLP.BatchSize = 64
	}
	if c.Telemetry.OTLP.FlushEvery == 0 {
		c.Telemetry.OTLP.FlushEvery = 5 * time.Second
	}
	if c.Telemetry.OTLP.ServiceName == "" {
		c.Telemetry.OTLP.ServiceName = "agentgate"
	}
}

func (c *Config) Validate() error {
	if len(c.Backends) == 0 {
		return errors.New("at least one backend is required")
	}
	for i, b := range c.Backends {
		if b.Name == "" {
			return fmt.Errorf("backends[%d].name is required", i)
		}
		if b.Type == "" {
			return fmt.Errorf("backends[%d].type is required", i)
		}
		if len(b.AllEndpoints()) == 0 && b.Type != "mock" && b.Type != "openai" && b.Type != "anthropic" {
			return fmt.Errorf("backends[%d] needs endpoint(s)", i)
		}
		if b.Type == "anthropic" && b.APIKey == "" {
			return fmt.Errorf("backends[%d] (anthropic) needs api_key", i)
		}
	}
	return nil
}

func (b BackendConfig) AllEndpoints() []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(v string) {
		if v == "" {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	add(b.Endpoint)
	for _, endpoint := range b.Endpoints {
		add(endpoint)
	}
	for _, endpoint := range b.Discovery.Endpoints {
		add(endpoint)
	}
	return out
}
