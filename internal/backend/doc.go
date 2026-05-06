// Package backend defines the stable interface between AgentGate's routing
// layer and concrete inference backends.
//
// The base Backend interface avoids assuming that every backend has instances
// or prefix-cache support. Optional capabilities such as instance selection are
// exposed through small side interfaces.
package backend
