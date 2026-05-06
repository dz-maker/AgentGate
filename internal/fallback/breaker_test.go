package fallback

import (
	"testing"
	"time"
)

func TestBreakerOpensAfterFailureThreshold(t *testing.T) {
	b := NewBreaker(Options{FailureThreshold: 3, Cooldown: time.Hour})
	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatalf("breaker should allow attempt %d", i)
		}
		b.Failure()
	}
	if !b.Allow() {
		t.Fatal("third attempt should still be allowed")
	}
	b.Failure()
	if b.Allow() {
		t.Fatal("breaker should be open")
	}
	if b.Snapshot().State != StateOpen {
		t.Fatalf("expected open, got %q", b.Snapshot().State)
	}
}

func TestBreakerHalfOpensAfterCooldownAndCloses(t *testing.T) {
	b := NewBreaker(Options{FailureThreshold: 1, SuccessThreshold: 2, Cooldown: 30 * time.Millisecond})
	b.Failure()
	if b.Allow() {
		t.Fatal("breaker must be open immediately")
	}
	time.Sleep(50 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("breaker should half-open after cooldown")
	}
	if b.Snapshot().State != StateHalfOpen {
		t.Fatalf("expected half_open, got %q", b.Snapshot().State)
	}
	b.Success()
	if b.Snapshot().State != StateHalfOpen {
		t.Fatalf("one success should not yet close: state=%q", b.Snapshot().State)
	}
	b.Success()
	if b.Snapshot().State != StateClosed {
		t.Fatalf("two successes should close: state=%q", b.Snapshot().State)
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	b := NewBreaker(Options{FailureThreshold: 1, Cooldown: 10 * time.Millisecond})
	b.Failure()
	time.Sleep(20 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected half-open allow")
	}
	b.Failure()
	if b.Snapshot().State != StateOpen {
		t.Fatalf("half-open failure should reopen: state=%q", b.Snapshot().State)
	}
}

func TestSetSharesBreakersAcrossCalls(t *testing.T) {
	s := NewSet(Options{FailureThreshold: 1, Cooldown: time.Hour})
	a := s.For("vllm-prod")
	b := s.For("vllm-prod")
	if a != b {
		t.Fatal("Set must return the same breaker for the same name")
	}
}
