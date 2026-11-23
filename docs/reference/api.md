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
  HMAC-signed and bound to the endpoint, the tenant and the query. Follow
  `next_cursor` until it is absent; the last page never comes back empty.
  A readings page is the exception: each request decrypts a bounded number
  of chunks, so a page can hold fewer readings than `limit`, or none, and
  still carry `next_cursor`. A readings query that runs past the server's
  deadline gets `503` with code `timeout`.
- **Rate limits**: per token (default 20 requests/s, burst 40), and per
  client network (IPv4 address or IPv6 /64) for failed token lookups
  (default 5/s, burst 50), per replica. Over a limit: `429` with
  `Retry-After`. Both are checked before the token is looked up, and the
  token's limit before its scopes, so a `429` can come where a `401` or
  `403` would otherwise be the answer. On the device listener, enrollment
  and `GET /v1/crl` each have a per-network limit and renewal a per-device
  one (default 50/s, burst 200).
- **Request bodies**: JSON, at most 1 MiB, unknown fields rejected.

## Scopes

| Scope | Allows |
|---|---|
| `devices:read` | list and show devices, device purge jobs |
| `devices:admin` | revoke and delete devices, create enrollment tokens |
| `readings:read` | readings and gaps |
| `exports:read` / `exports:write` | export status, download and export jobs / create exports |
| `audit:read` | the tenant's audit log |
| `tenant:admin` | the tenant's settings and API tokens, retention and tenant destroy jobs |

## Jobs

Deleting a device or a tenant, exports and retention run as background
jobs. `DELETE /v1/devices/{id}` and `DELETE /v1/admin/tenants/{id}` answer
`202` with the `job_id` doing the work: a new job, or the one already
queued, running or done for the same device or tenant. A job that failed
for good does not block a new request. `GET /v1/jobs/{id}` needs the read
scope of what the job works on (see above), and its `last_error` is a
fixed message; the real error is in the server log under the job ID.

The API deletes finished jobs 30 days after they finish, and `GET` then
answers `404`. It deletes enrollment tokens 7 days after they expire,
used or not.

## Exports

A tenant can have 4 exports pending or running at once
(`FLEETWIRE_API_MAX_ACTIVE_EXPORTS`); another `POST /v1/exports` gets `409`
with code `too_many_exports`. A download past the export's `expires_at`
gets `410` with code `expired`. `HEAD` on the download sends the status and
headers, `Content-Digest` included, without the file. The API deletes an
export 30 days after its `expires_at`, or 30 days after its `created_at`
if it failed; `GET /v1/exports/{id}` and the download then answer `404`.
