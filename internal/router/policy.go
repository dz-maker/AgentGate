package router

import (
	"context"

	"github.com/agentgate/agentgate/internal/backend"
	"github.com/agentgate/agentgate/internal/cache/prefix"
	"github.com/agentgate/agentgate/pkg/types"
)

type Decision struct {
	Backend        backend.Backend
	InstanceID     string
	PrefixMatch    prefix.Match
	PrefixSegments []prefix.Segment
}

type Router struct {
	registry *backend.Registry
	prefix   *prefix.Service
}

func New(registry *backend.Registry, prefixSvc *prefix.Service) *Router {
	return &Router{registry: registry, prefix: prefixSvc}
}

func (r *Router) Route(ctx context.Context, req types.Request) (Decision, error) {
	b, err := r.registry.Default()
	if err != nil {
		return Decision{}, err
	}
	return r.RouteBackend(ctx, req, b)
}

func (r *Router) RouteBackend(ctx context.Context, req types.Request, b backend.Backend) (Decision, error) {
	if b == nil {
		return Decision{}, backend.ErrNoHealthyBackend
	}

	var segments []prefix.Segment
	var match prefix.Match
	if r.prefix != nil && b.Capabilities().SupportsPrefixCache {
		segments = r.prefix.Extract(req)
		match = r.prefix.Lookup(req.TenantID, segments)
	} else {
		match = prefix.Match{Reason: "prefix_unsupported"}
	}

	var instanceID string
	if selector, ok := b.(backend.InstanceSelector); ok {
		var err error
		instanceID, err = selector.SelectInstance(ctx, backend.RoutingHint{
			PreferredInstance: match.BackendID,
			PrefixTokens:      match.MatchedTokens,
			SessionID:         req.SessionID,
			TenantID:          req.TenantID,
		})
		if err != nil {
			return Decision{}, err
		}
	}

	return Decision{
		Backend:        b,
		InstanceID:     instanceID,
		PrefixMatch:    match,
		PrefixSegments: segments,
	}, nil
}

func (r *Router) Feedback(req types.Request, decision Decision) {
	if r.prefix == nil || decision.InstanceID == "" || !decision.Backend.Capabilities().SupportsPrefixCache {
		return
	}
	r.prefix.Insert(req.TenantID, decision.PrefixSegments, decision.InstanceID)
}
