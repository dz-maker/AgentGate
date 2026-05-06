package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAppliesDefaultsAndExpandsEnv(t *testing.T) {
	t.Setenv("AGENTGATE_TEST_TOKEN", "secret")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(`
backends:
  - name: vllm
    type: vllm
    endpoint: http://localhost:8000
    headers:
      Authorization: "Bearer ${AGENTGATE_TEST_TOKEN}"
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Addr != ":9000" {
		t.Fatalf("unexpected default addr %q", cfg.Server.Addr)
	}
	if cfg.Prefix.Enabled == nil || !*cfg.Prefix.Enabled {
		t.Fatal("prefix cache should default to enabled")
	}
	if cfg.ToolParser.Enabled == nil || !*cfg.ToolParser.Enabled {
		t.Fatal("tool parser should default to enabled")
	}
	if cfg.Prefix.HalfLife != 5*time.Minute {
		t.Fatalf("unexpected half life %s", cfg.Prefix.HalfLife)
	}
	if got := cfg.Backends[0].Headers["Authorization"]; got != "Bearer secret" {
		t.Fatalf("env header was not expanded: %q", got)
	}
}
