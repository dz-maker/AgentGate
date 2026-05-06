// Package otel exports AgentGate trace spans to an OpenTelemetry
// collector over OTLP-HTTP.
//
// The implementation is deliberately minimal: we marshal the OTLP
// JSON-Protobuf envelope ourselves rather than depending on
// go.opentelemetry.io/otel and its 30+ transitive modules. The reason is
// twofold:
//
//   - go.mod is intentionally tiny (see BACKGROUND.md §7); the OTel SDK
//     would dwarf every other dep combined.
//   - Our use case is "fan out spans to a collector," not the full SDK
//     surface (samplers, propagators, metrics, baggage). The OTLP-HTTP
//     wire format is small and stable enough to write by hand, and the
//     collector does the heavy lifting downstream.
//
// If a future user genuinely needs the SDK (e.g. context propagation
// inside another Go service), this package's Exporter is the seam to
// replace.
package otel
