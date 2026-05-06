// Package router turns normalized Agent requests into backend routing decisions.
//
// It is capability-aware: prefix locality hints are only computed for backends
// that advertise prefix-cache support, and concrete instance selection is used
// only when a backend implements the optional backend.InstanceSelector interface.
package router
