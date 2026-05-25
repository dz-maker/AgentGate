package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/agentgate/agentgate/internal/api/handler"
	"github.com/agentgate/agentgate/internal/backend"
	"github.com/agentgate/agentgate/internal/backend/anthropic"
	"github.com/agentgate/agentgate/internal/backend/mock"
	"github.com/agentgate/agentgate/internal/backend/ollama"
	"github.com/agentgate/agentgate/internal/backend/openai"
	"github.com/agentgate/agentgate/internal/backend/sglang"
	"github.com/agentgate/agentgate/internal/backend/vllm"
	"github.com/agentgate/agentgate/internal/cache/prefix"
	"github.com/agentgate/agentgate/internal/cache/semantic"
	"github.com/agentgate/agentgate/internal/capability"
	"github.com/agentgate/agentgate/internal/config"
	"github.com/agentgate/agentgate/internal/fallback"
	"github.com/agentgate/agentgate/internal/observe/otel"
	agenttrace "github.com/agentgate/agentgate/internal/observe/trace"
	"github.com/agentgate/agentgate/internal/policy"
	"github.com/agentgate/agentgate/internal/router"
	"github.com/agentgate/agentgate/pkg/types"
)

func main() {
	configPath := flag.String("c", "configs/agentgate.example.yaml", "config file")
	addr := flag.String("addr", "", "override listen address")
	showVersion := flag.Bool("version", false, "print build info and exit")
	flag.Parse()

	if *showVersion {
		printVersion(os.Stdout)
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}
	if *addr != "" {
		cfg.Server.Addr = *addr
	}

	registry, err := buildRegistry(cfg)
	if err != nil {
		logger.Error("build backend registry", "err", err)
		os.Exit(1)
	}

	var prefixSvc *prefix.Service
	if cfg.Prefix.Enabled != nil && *cfg.Prefix.Enabled {
		prefixSvc = prefix.NewService(prefix.Options{
			MaxEntries:   cfg.Prefix.MaxEntries,
			HalfLife:     cfg.Prefix.HalfLife,
			DebugContent: cfg.Prefix.DebugContent,
		})
	}

	var semanticSvc *semantic.Service
	if cfg.Semantic.Enabled != nil && *cfg.Semantic.Enabled {
		semanticSvc = semantic.New(semantic.Options{
			MaxEntries: cfg.Semantic.MaxEntries,
			TTLExact:   cfg.Semantic.TTLExact,
			TTLTool:    cfg.Semantic.TTLTool,
		})
	}

	policyEngine, err := policy.LoadFromFile(cfg.Policy.Path)
	if err != nil {
		logger.Error("load policy", "err", err)
		os.Exit(1)
	}

	caps := capability.NewRegistry()
	for _, b := range registry.All() {
		caps.Register(b.Name(), b.Capabilities(), nil)
	}

	costModel := router.NewCostModel()
	breakers := fallback.NewSet(fallback.Options{
		FailureThreshold: cfg.Fallback.FailureThreshold,
		SuccessThreshold: cfg.Fallback.SuccessThreshold,
		Cooldown:         cfg.Fallback.Cooldown,
	})

	traceStore := agenttrace.NewStore(cfg.TraceDir)
	defer traceStore.Close()
	otelClose, err := attachOTLPSink(cfg.Telemetry.OTLP, traceStore, logger)
	if err != nil {
		logger.Error("otel exporter", "err", err)
		os.Exit(1)
	}
	defer otelClose()

	r := router.New(registry, prefixSvc)
	api := handler.New(handler.Options{
		Router:          r,
		Registry:        registry,
		Prefix:          prefixSvc,
		Semantic:        semanticSvc,
		Policy:          policyEngine,
		Breakers:        breakers,
		Caps:            caps,
		Cost:            costModel,
		TraceStore:      traceStore,
		Replay:          agenttrace.NewReplay(cfg.TraceDir),
		EnableToolParse:  cfg.ToolParser.Enabled != nil && *cfg.ToolParser.Enabled,
		ParserBuffer:     cfg.ToolParser.MaxBufferBytes,
		ParserAggressive: cfg.ToolParser.AggressiveAbort,
		Logger:           logger,
	})

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           api.Routes(),
		ReadHeaderTimeout: cfg.Timeouts.Header,
		IdleTimeout:       90 * time.Second,
		// WriteTimeout is intentionally left unset because SSE streams may stay
		// open for minutes while a model is decoding.
	}

	go func() {
		logger.Info("agentgate listening", "addr", cfg.Server.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown", "err", err)
		os.Exit(1)
	}
	if err := registry.Close(); err != nil {
		logger.Warn("close backends", "err", err)
	}
	logger.Info("agentgate stopped")
}

func buildRegistry(cfg *config.Config) (*backend.Registry, error) {
	var backends []backend.Backend
	for _, item := range cfg.Backends {
		switch strings.ToLower(item.Type) {
		case "mock":
			backends = append(backends, mock.New(item.Name))
		case "vllm":
			b, err := vllm.New(vllmOpts(item, cfg.Timeouts))
			if err != nil {
				return nil, err
			}
			backends = append(backends, b)
		case "sglang":
			b, err := sglang.New(vllmOpts(item, cfg.Timeouts))
			if err != nil {
				return nil, err
			}
			backends = append(backends, b)
		case "ollama":
			ep := firstEndpoint(item)
			if ep == "" {
				return nil, fmt.Errorf("ollama backend %q needs an endpoint", item.Name)
			}
			b, err := ollama.New(ollama.Options{
				Name:          item.Name,
				Endpoint:      ep,
				Headers:       item.Headers,
				HeaderTimeout: cfg.Timeouts.Header,
			})
			if err != nil {
				return nil, err
			}
			backends = append(backends, b)
		case "openai":
			b, err := openai.New(openai.Options{
				Name:          item.Name,
				Endpoint:      firstEndpoint(item),
				APIKey:        item.APIKey,
				Headers:       item.Headers,
				Vendor:        item.Vendor,
				Models:        item.Models,
				Cost:          types.CostProfile(item.Cost),
				HeaderTimeout: cfg.Timeouts.Header,
			})
			if err != nil {
				return nil, err
			}
			backends = append(backends, b)
		case "anthropic":
			b, err := anthropic.New(anthropic.Options{
				Name:          item.Name,
				Endpoint:      firstEndpoint(item),
				APIKey:        item.APIKey,
				Models:        item.Models,
				Cost:          types.CostProfile(item.Cost),
				Headers:       item.Headers,
				HeaderTimeout: cfg.Timeouts.Header,
			})
			if err != nil {
				return nil, err
			}
			backends = append(backends, b)
		default:
			return nil, fmt.Errorf("unsupported backend type %q", item.Type)
		}
	}
	return backend.NewRegistry(backends), nil
}

// printVersion writes a short build summary using the module info Go
// embeds at build time. With `go install`, this includes the module
// version; with `go build` from source, it falls back to the VCS revision
// recorded by the toolchain.
func printVersion(w *os.File) {
	version := "devel"
	revision := ""
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = info.Main.Version
		}
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				revision = s.Value
				break
			}
		}
	}
	fmt.Fprintf(w, "agentgate %s", version)
	if revision != "" {
		short := revision
		if len(short) > 12 {
			short = short[:12]
		}
		fmt.Fprintf(w, " (%s)", short)
	}
	fmt.Fprintf(w, " %s/%s %s\n", runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// attachOTLPSink wires an OTLP exporter into traceStore when an
// endpoint is configured. The returned cleanup is always safe to defer,
// even when no exporter was attached (it is a no-op in that case).
func attachOTLPSink(cfg config.OTLPConfig, traceStore *agenttrace.Store, logger *slog.Logger) (func(), error) {
	if cfg.Endpoint == "" {
		return func() {}, nil
	}
	exp, err := otel.New(otel.Options{
		Endpoint:    cfg.Endpoint,
		Headers:     cfg.Headers,
		ServiceName: cfg.ServiceName,
		BatchSize:   cfg.BatchSize,
		FlushEvery:  cfg.FlushEvery,
		ErrorFn:     func(err error) { logger.Warn("otel export", "err", err) },
	})
	if err != nil {
		return func() {}, err
	}
	traceStore.AddSink(exp)
	return func() { _ = exp.Close(context.Background()) }, nil
}

// firstEndpoint returns the first configured endpoint for backends that
// only accept a single upstream URL (openai, anthropic). Returns "" if
// none is configured, letting the adapter fall back to its vendor default.
func firstEndpoint(b config.BackendConfig) string {
	if eps := b.AllEndpoints(); len(eps) > 0 {
		return eps[0]
	}
	return ""
}

func vllmOpts(item config.BackendConfig, timeouts config.TimeoutConfig) vllm.Options {
	return vllm.Options{
		Name:           item.Name,
		Endpoints:      item.AllEndpoints(),
		Headers:        item.Headers,
		HeaderTimeout:  timeouts.Header,
		HealthTimeout:  timeouts.HealthCheck,
		HealthInterval: 10 * time.Second,
		Models:         item.Models,
	}
}
