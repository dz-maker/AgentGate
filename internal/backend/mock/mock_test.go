package mock

import (
	"context"
	"testing"

	"github.com/agentgate/agentgate/pkg/types"
)

func TestStreamCancellation(t *testing.T) {
	b := New("mock")
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := b.Stream(ctx, &types.Request{Model: "mock"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for range stream {
	}
}
