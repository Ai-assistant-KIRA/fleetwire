-- Schema for fleetwire. Every tenant table carries tenant_id so that
-- row-level security (00002) can scope it. Runtime services never connect as
-- the role that runs these migrations.

-- +goose Up
-- btree_gist is a trusted extension: the database owner can create it. It
-- provides the gist equality operator for uuid used by the exclusion
-- constraint on chunks.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE fleetwire.tenants (
    id             uuid PRIMARY KEY,
    name           text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    status         text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'destroying', 'destroyed')),
    retention_days int  NOT NULL DEFAULT 30 CHECK (retention_days BETWEEN 1 AND 3650),
    created_at     timestamptz NOT NULL DEFAULT now(),
    destroyed_at   timestamptz
);

-- Wrapped data keys. wrapped_dek becomes NULL when the tenant is destroyed;
-- the row stays as a tombstone.
CREATE TABLE fleetwire.tenant_keys (
    tenant_id    uuid   NOT NULL REFERENCES fleetwire.tenants (id),
    key_id       uuid   NOT NULL,
    kek_id       text   NOT NULL,
    wrapped_dek  bytea,
    seal_count   bigint NOT NULL DEFAULT 0 CHECK (seal_count >= 0),
    created_at   timestamptz NOT NULL DEFAULT now(),
    retired_at   timestamptz,
    destroyed_at timestamptz,
    PRIMARY KEY (tenant_id, key_id)
);

CREATE TABLE fleetwire.devices (
    id             uuid PRIMARY KEY,
    tenant_id      uuid   NOT NULL REFERENCES fleetwire.tenants (id),
    name           text   NOT NULL DEFAULT '' CHECK (length(name) <= 200),
    status         text   NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked', 'deleted')),
    acked_through  bigint NOT NULL DEFAULT 0 CHECK (acked_through >= 0),
    last_seen_at   timestamptz,
    last_batch_at  timestamptz,
    cert_serial    text,
    cert_not_after timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    revoked_at     timestamptz,
    -- Target of the composite foreign keys below, which make it impossible
    -- to attach a device's data to another tenant.
    UNIQUE (tenant_id, id)
);

-- Covered seq ranges above acked_through (ADR-0005). Normally empty.
CREATE TABLE fleetwire.device_ranges (
    tenant_id uuid   NOT NULL,
    device_id uuid   NOT NULL,
    lo        bigint NOT NULL,
    hi        bigint NOT NULL,
    CHECK (1 <= lo AND lo <= hi),
    PRIMARY KEY (device_id, lo),
    FOREIGN KEY (tenant_id, device_id) REFERENCES fleetwire.devices (tenant_id, id) ON DELETE CASCADE
);

-- Every gap a device reported, as recorded (only the parts not stored).
CREATE TABLE fleetwire.device_gaps (
    tenant_id   uuid   NOT NULL,
    device_id   uuid   NOT NULL,
    from_seq    bigint NOT NULL,
    to_seq      bigint NOT NULL,
    reason      text   NOT NULL CHECK (reason IN ('UNSPECIFIED', 'DISK_BUDGET', 'CORRUPTION', 'LOST')),
    reported_at timestamptz NOT NULL DEFAULT now(),
    CHECK (1 <= from_seq AND from_seq <= to_seq),
    PRIMARY KEY (device_id, from_seq),
    EXCLUDE USING gist (device_id WITH =, int8range(from_seq, to_seq, '[]') WITH &&),
    FOREIGN KEY (tenant_id, device_id) REFERENCES fleetwire.devices (tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX device_gaps_retention ON fleetwire.device_gaps (tenant_id, reported_at);

-- Encrypted readings, one row per stored seq range.
CREATE TABLE fleetwire.chunks (
    tenant_id        uuid   NOT NULL,
    device_id        uuid   NOT NULL,
    seq_lo           bigint NOT NULL,
    seq_hi           bigint NOT NULL,
    reading_count    int    NOT NULL,
    min_collected_at timestamptz NOT NULL,
    max_collected_at timestamptz NOT NULL,
    key_id           uuid   NOT NULL,
    nonce            bytea  NOT NULL,
    ciphertext       bytea  NOT NULL,
    received_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (1 <= seq_lo AND seq_lo <= seq_hi),
    CHECK (reading_count = seq_hi - seq_lo + 1),
    CHECK (min_collected_at <= max_collected_at),
    PRIMARY KEY (device_id, seq_lo),
    -- The last line of defense for "never stored twice" (ADR-0005).
    CONSTRAINT chunks_no_overlap EXCLUDE USING gist (device_id WITH =, int8range(seq_lo, seq_hi, '[]') WITH &&),
    FOREIGN KEY (tenant_id, device_id) REFERENCES fleetwire.devices (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, key_id) REFERENCES fleetwire.tenant_keys (tenant_id, key_id)
);
CREATE INDEX chunks_by_time ON fleetwire.chunks (tenant_id, device_id, max_collected_at);
CREATE INDEX chunks_retention ON fleetwire.chunks (tenant_id, max_collected_at);

-- Issued device certificates. Rows are kept until the certificate expires,
-- even after the device or tenant is gone, so revoked certificates stay on
-- the CRL for as long as they could be presented.
CREATE TABLE fleetwire.device_certs (
    serial     text PRIMARY KEY,
    tenant_id  uuid NOT NULL,
    device_id  uuid NOT NULL,
    not_after  timestamptz NOT NULL,
    issued_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);
CREATE INDEX device_certs_by_device ON fleetwire.device_certs (device_id);
CREATE INDEX device_certs_revoked ON fleetwire.device_certs (not_after) WHERE revoked_at IS NOT NULL;

CREATE TABLE fleetwire.enrollment_tokens (
    id             uuid  PRIMARY KEY,
    tenant_id      uuid  NOT NULL REFERENCES fleetwire.tenants (id),
    token_hash     bytea NOT NULL UNIQUE,
    expires_at     timestamptz NOT NULL,
    used_at        timestamptz,
    used_by_device uuid,
    created_by     text  NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE fleetwire.api_tokens (
    id           uuid   PRIMARY KEY,
    tenant_id    uuid   NOT NULL REFERENCES fleetwire.tenants (id),
    name         text   NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    token_hash   bytea  NOT NULL,
    scopes       text[] NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at   timestamptz
);
CREATE INDEX api_tokens_by_tenant ON fleetwire.api_tokens (tenant_id, created_at);

-- Operator tokens are fleet-wide, so they are not tenant scoped. The runtime
-- role has no privileges on this table at all; it reads it only through
-- operator_token_lookup().
CREATE TABLE fleetwire.operator_tokens (
    id           uuid  PRIMARY KEY,
    name         text  NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    token_hash   bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at   timestamptz
);

CREATE TABLE fleetwire.jobs (
    id           uuid  PRIMARY KEY,
    tenant_id    uuid  NOT NULL REFERENCES fleetwire.tenants (id),
    kind         text  NOT NULL CHECK (kind IN ('retention', 'export', 'device_purge', 'tenant_destroy')),
    status       text  NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    payload      jsonb NOT NULL DEFAULT '{}',
    result       jsonb,
    attempts     int   NOT NULL DEFAULT 0,
    max_attempts int   NOT NULL DEFAULT 20,
    last_error   text,
    run_after    timestamptz NOT NULL DEFAULT now(),
    locked_until timestamptz,
    locked_by    text,
    dedupe_key   text UNIQUE,
    created_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz
);
CREATE INDEX jobs_runnable ON fleetwire.jobs (run_after) WHERE status IN ('queued', 'running');

CREATE TABLE fleetwire.exports (
    id            uuid PRIMARY KEY,
    tenant_id     uuid NOT NULL REFERENCES fleetwire.tenants (id),
    device_id     uuid,
    status        text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'ready', 'failed', 'expired')),
    file_path     text,
    sha256        bytea,
    bytes         bigint,
    reading_count bigint,
    error         text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz,
    expires_at    timestamptz
);
CREATE INDEX exports_by_tenant ON fleetwire.exports (tenant_id, created_at);

-- Append-only audit log (R9.6). See 00003 for the trigger that blocks
-- updates and deletes.
CREATE TABLE fleetwire.audit_log (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id   uuid REFERENCES fleetwire.tenants (id),
    at          timestamptz NOT NULL DEFAULT now(),
    actor_type  text  NOT NULL CHECK (actor_type IN ('operator', 'tenant_token', 'device', 'system')),
    actor_id    text  NOT NULL,
    action      text  NOT NULL,
    target_type text  NOT NULL,
    target_id   text  NOT NULL,
    result      text  NOT NULL CHECK (result IN ('success', 'denied', 'failure')),
    details     jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX audit_log_by_tenant ON fleetwire.audit_log (tenant_id, id);

-- +goose Down
DROP TABLE fleetwire.audit_log;
DROP TABLE fleetwire.exports;
DROP TABLE fleetwire.jobs;
DROP TABLE fleetwire.operator_tokens;
DROP TABLE fleetwire.api_tokens;
DROP TABLE fleetwire.enrollment_tokens;
DROP TABLE fleetwire.device_certs;
DROP TABLE fleetwire.chunks;
DROP TABLE fleetwire.device_gaps;
DROP TABLE fleetwire.device_ranges;
DROP TABLE fleetwire.devices;
DROP TABLE fleetwire.tenant_keys;
DROP TABLE fleetwire.tenants;
