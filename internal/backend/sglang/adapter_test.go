package sglang

import (
	"testing"

	"github.com/agentgate/agentgate/internal/backend/vllm"
	"github.com/agentgate/agentgate/pkg/types"
)

func TestSGLangAdvertisesRadixPrefixMode(t *testing.T) {
	a, err := New(vllm.Options{
		Name:      "sglang-edge",
		Endpoints: []string{"http://localhost:30000"},
	})
	if err != nil {
		t.Fatal(err)
	}
	caps := a.Capabilities()
	if caps.Vendor != "sglang" {
		t.Fatalf("vendor must be sglang, got %q", caps.Vendor)
	}
	if caps.PrefixCacheMode != types.PrefixCacheRadix {
		t.Fatalf("expected radix prefix cache mode, got %q", caps.PrefixCacheMode)
	}
}

func TestSGLangRespectsExplicitOverrides(t *testing.T) {
	a, err := New(vllm.Options{
		Name:       "sglang-external-kv",
		Endpoints:  []string{"http://localhost:30000"},
		PrefixMode: types.PrefixCacheExternalKV,
		KVProvider: "lmcache",
	})
	if err != nil {
		t.Fatal(err)
	}
	caps := a.Capabilities()
	if caps.PrefixCacheMode != types.PrefixCacheExternalKV {
		t.Fatalf("expected external_kv override, got %q", caps.PrefixCacheMode)
	}
	if caps.KVProvider != "lmcache" {
		t.Fatalf("expected lmcache override, got %q", caps.KVProvider)
	}
}
