package capability

import (
	"context"
	"errors"
	"testing"

	"github.com/agentgate/agentgate/pkg/types"
)

type staticProber struct {
	caps types.Capabilities
	err  error
}

func (s staticProber) Probe(context.Context) (types.Capabilities, error) {
	return s.caps, s.err
}

func TestRegistryRegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	reg.Register("vllm", types.Capabilities{
		Vendor:          "vllm",
		PrefixCacheMode: types.PrefixCacheAPC,
	}, nil)

	got, ok := reg.Get("vllm")
	if !ok {
		t.Fatal("expected sheet")
	}
	if !got.SupportsPrefixSticky() {
		t.Fatal("APC backend should support sticky")
	}
	if got.Caps.Vendor != "vllm" {
		t.Fatalf("vendor mismatch: %s", got.Caps.Vendor)
	}
}

func TestRegistryUnhealthyHidesPrefixSticky(t *testing.T) {
	reg := NewRegistry()
	reg.Register("vllm", types.Capabilities{PrefixCacheMode: types.PrefixCacheAPC}, nil)
	reg.Update("vllm", types.Capabilities{PrefixCacheMode: types.PrefixCacheAPC}, false)

	got, _ := reg.Get("vllm")
	if got.SupportsPrefixSticky() {
		t.Fatal("unhealthy backend should not advertise sticky prefix")
	}
}

func TestRegistryRefreshAllPropagatesProbeFailure(t *testing.T) {
	reg := NewRegistry()
	reg.Register("ollama",
		types.Capabilities{Vendor: "ollama"},
		staticProber{err: errors.New("connect refused")},
	)
	reg.RefreshAll(context.Background())

	got, _ := reg.Get("ollama")
	if got.Healthy {
		t.Fatal("expected unhealthy after probe error")
	}
}

func TestRegistryAllSortedByName(t *testing.T) {
	reg := NewRegistry()
	reg.Register("zeta", types.Capabilities{}, nil)
	reg.Register("alpha", types.Capabilities{}, nil)
	reg.Register("mu", types.Capabilities{}, nil)
	all := reg.All()
	if len(all) != 3 || all[0].Backend != "alpha" || all[2].Backend != "zeta" {
		t.Fatalf("expected sorted: %+v", all)
	}
}
