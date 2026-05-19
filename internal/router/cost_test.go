package router

import (
	"testing"
	"time"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestCostModelPrefersCheaperBackend(t *testing.T) {
	cm := NewCostModel()
	expensive := types.Capabilities{Vendor: "anthropic", CostProfile: types.CostProfile{
		InputUSDPer1K: 0.003, OutputUSDPer1K: 0.015,
	}}
	cheap := types.Capabilities{Vendor: "openai", CostProfile: types.CostProfile{
		InputUSDPer1K: 0.001, OutputUSDPer1K: 0.002,
	}}
	if cm.Score("a", expensive) <= cm.Score("o", cheap) {
		t.Fatal("expected anthropic to score higher (more expensive)")
	}
}

func TestCostModelSelfHostedFallsBackToLatency(t *testing.T) {
	cm := NewCostModel()
	// Two backends, both zero cost. Slower one should score higher.
	caps := types.Capabilities{Vendor: "vllm"}
	cm.Observe("fast", 1000, 100*time.Millisecond)
	cm.Observe("slow", 1000, 800*time.Millisecond)
	if cm.Score("slow", caps) <= cm.Score("fast", caps) {
		t.Fatal("slow vllm instance should score higher than fast one")
	}
}

func TestCostModelEWMAMakesObservationsConverge(t *testing.T) {
	cm := NewCostModel()
	caps := types.Capabilities{}
	for i := 0; i < 50; i++ {
		cm.Observe("b", 1000, 100*time.Millisecond)
	}
	first := cm.Score("b", caps)
	cm.Observe("b", 1000, 100*time.Millisecond)
	second := cm.Score("b", caps)
	if diff := abs(first - second); diff > 0.0001 {
		t.Fatalf("EWMA should converge: %f vs %f", first, second)
	}
}

func TestCostModelObservesSubMillisecondLatency(t *testing.T) {
	cm := NewCostModel()
	cm.Observe("b", 1000, 500*time.Microsecond)
	snap := cm.Snapshot()["b"]
	if snap.MeanLatency <= 0 {
		t.Fatalf("sub-ms latency should be recorded, got %f", snap.MeanLatency)
	}
	if snap.MeanLatency >= 1 {
		t.Fatalf("500us should record as <1ms, got %f", snap.MeanLatency)
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
