// Package meldbase provides an embedded, durable, realtime document database.
//
// It is the stable public Go API for Meldbase. HTTP/WebSocket serving is
// provided by github.com/crapthings/meldbase/server; optional deployment
// adapters live below github.com/crapthings/meldbase/integrations.
//
//go:generate go run ./internal/generate/rootapi
package meldbase
