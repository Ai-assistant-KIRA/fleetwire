# ADR-0011: Standard library HTTP routing, no framework

- Status: accepted
- Date: 2026-11-09

## Context

The API has about 30 routes, three listeners, and a fixed middleware chain:
request ID, panic recovery, access log and metrics, tracing, authentication,
rate limiting. Since Go 1.22, `net/http.ServeMux` matches methods and path
wildcards (`GET /v1/devices/{id}`), and `Request.Pattern` names the matched
route for metrics.

## Decision

Use `net/http` directly. Middleware is plain `http.Handler` wrapping. Each
route is registered with the scope it needs (`tenantRoute(scope, h)` or
`operatorRoute(h)`), so a route cannot be added without stating who may call
it. The response recorder turns the mux's own plain-text 404 and 405 replies
into RFC 9457 problem details, so every error has the same shape.

## Alternatives considered

- **chi, gin, echo.** They add little over the standard mux for this API and
  bring their own context types and middleware conventions.
- **Generated server from the OpenAPI file.** Keeps code and spec in step,
  but the generated handlers fight the per-route auth design. The contract
  tests check every real response against the spec instead.

## Consequences

- No third-party router to update or audit.
- Route metrics use the pattern (`GET /v1/devices/{id}`), never the raw path,
  so cardinality stays bounded.

## Note, 2026-11-22

The API now has four listeners. The fourth, the keyring listener for
ingest (off by default, `:8445` on Kubernetes), came later with the remote
keyring of ADR-0007. It serves two routes from `internal/crypto/keyring`
on a mux of its own, behind mutual TLS and an allowlist of client
certificate names instead of the middleware chain above
([listeners](../reference/api.md#listeners)).
