# API

The API is described in OpenAPI 3.1: [`api/openapi.yaml`](https://github.com/Ai-assistant-KIRA/fleetwire/blob/main/api/openapi.yaml).
The contract test (`internal/api/contract_test.go`) calls every documented
operation, success and error paths, and validates each real response
against the file; an operation the test never calls fails it.

## Listeners

| Listener | Default | TLS | Auth |
|---|---|---|---|
| public | `:8443` | server certificate | `Authorization: Bearer fwt_…` (tenant) or `fwo_…` (operator) |
| device | `:8444` | server certificate, client certificate requested | enrollment token, or the device certificate for renewal; `GET /v1/crl` is public |
| internal | off; `:8445` in Kubernetes | server certificate, client certificate required (service CA) | CN `fleetwire-ingest` only: keyring wrap and unwrap |
| admin | `127.0.0.1:9090` | none | none: `/healthz`, `/readyz`, `/metrics` |

## Conventions

- **Errors** are RFC 9457 `application/problem+json` with `type`, `title`,
  `status`, `detail`, `instance` (the request ID) and a stable `code`.
  Internal error messages never reach the client; the request ID finds
  them in the server log.
- **Paging**: lists return `items` and `next_cursor`. Cursors are opaque,
  HMAC-signed and bound to the endpoint, the tenant and the query.
- **Rate limits**: per token (default 20 requests/s, burst 40), per
  replica. Over the limit: `429` with `Retry-After`.
- **Request bodies**: JSON, at most 1 MiB, unknown fields rejected.

## Scopes

| Scope | Allows |
|---|---|
| `devices:read` | list and show devices, job status |
| `devices:admin` | revoke and delete devices, create enrollment tokens |
| `readings:read` | readings and gaps |
| `exports:read` / `exports:write` | export status and download / create exports |
| `audit:read` | the tenant's audit log |
| `tenant:admin` | the tenant's settings and API tokens |
