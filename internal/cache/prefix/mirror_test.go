package prefix

import (
	"testing"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestServiceFanoutsToMirror(t *testing.T) {
	mirror := NewLocalMirror(16)
	svc := NewService(Options{MaxEntries: 100, Mirror: mirror})

	req := types.Request{
		TenantID: "tenant-a",
		Messages: []types.Message{
			{Role: types.RoleSystem, Content: "you are tools"},
			{Role: types.RoleUser, Content: "hi"},
		},
	}
	segs := svc.Extract(req)
	svc.Insert("tenant-a", segs, "vllm-0")

	events := mirror.Drain()
	if len(events) != 1 {
		t.Fatalf("expected 1 mirror event, got %d", len(events))
	}
	if events[0].BackendID != "vllm-0" {
		t.Fatalf("backend id not propagated: %+v", events[0])
	}
}

func TestLocalMirrorBoundsBufferSize(t *testing.T) {
	mirror := NewLocalMirror(2)
	for i := 0; i < 10; i++ {
		mirror.MirrorInsert("t", []Segment{{Hash: uint64(i)}}, "b")
	}
	stats := mirror.Stats()
	if stats.Pending != 2 {
		t.Fatalf("buffer size cap not enforced: %+v", stats)
	}
	if stats.Dropped == 0 {
		t.Fatalf("expected drops to be counted: %+v", stats)
	}
}
