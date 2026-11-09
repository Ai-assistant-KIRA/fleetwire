package store

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Tenant states.
const (
	TenantActive     = "active"
	TenantDestroying = "destroying"
	TenantDestroyed  = "destroyed"
)

// Device states.
const (
	DeviceActive  = "active"
	DeviceRevoked = "revoked"
	DeviceDeleted = "deleted"
)

// Tenant is a customer.
type Tenant struct {
	ID            uuid.UUID  `db:"id"`
	Name          string     `db:"name"`
	Status        string     `db:"status"`
	RetentionDays int        `db:"retention_days"`
	CreatedAt     time.Time  `db:"created_at"`
	DestroyedAt   *time.Time `db:"destroyed_at"`
}

// TenantKey is one wrapped data key.
type TenantKey struct {
	TenantID    uuid.UUID  `db:"tenant_id"`
	KeyID       uuid.UUID  `db:"key_id"`
	KEKID       string     `db:"kek_id"`
	WrappedDEK  []byte     `db:"wrapped_dek"`
	SealCount   int64      `db:"seal_count"`
	CreatedAt   time.Time  `db:"created_at"`
	RetiredAt   *time.Time `db:"retired_at"`
	DestroyedAt *time.Time `db:"destroyed_at"`
}

// Device is one enrolled machine.
type Device struct {
	ID           uuid.UUID  `db:"id"`
	TenantID     uuid.UUID  `db:"tenant_id"`
	Name         string     `db:"name"`
	Status       string     `db:"status"`
	AckedThrough uint64     `db:"acked_through"`
	LastSeenAt   *time.Time `db:"last_seen_at"`
	LastBatchAt  *time.Time `db:"last_batch_at"`
	CertSerial   *string    `db:"cert_serial"`
	CertNotAfter *time.Time `db:"cert_not_after"`
	CreatedAt    time.Time  `db:"created_at"`
	RevokedAt    *time.Time `db:"revoked_at"`
}

// IngestDevice is what ingest learns about a device under its row lock.
type IngestDevice struct {
	TenantID     uuid.UUID `db:"tenant_id"`
	DeviceStatus string    `db:"device_status"`
	TenantStatus string    `db:"tenant_status"`
	AckedThrough uint64    `db:"acked_through"`
}

// DeviceRef is a device's tenant and state, found without a tenant context.
type DeviceRef struct {
	TenantID     uuid.UUID `db:"tenant_id"`
	DeviceStatus string    `db:"device_status"`
	TenantStatus string    `db:"tenant_status"`
}

// Chunk is one encrypted seq range of readings.
type Chunk struct {
	TenantID       uuid.UUID `db:"tenant_id"`
	DeviceID       uuid.UUID `db:"device_id"`
	SeqLo          uint64    `db:"seq_lo"`
	SeqHi          uint64    `db:"seq_hi"`
	ReadingCount   int       `db:"reading_count"`
	MinCollectedAt time.Time `db:"min_collected_at"`
	MaxCollectedAt time.Time `db:"max_collected_at"`
	KeyID          uuid.UUID `db:"key_id"`
	Nonce          []byte    `db:"nonce"`
	Ciphertext     []byte    `db:"ciphertext"`
	ReceivedAt     time.Time `db:"received_at"`
}

// DeviceGap is a seq range a device reported it will never send.
type DeviceGap struct {
	TenantID   uuid.UUID `db:"tenant_id"`
	DeviceID   uuid.UUID `db:"device_id"`
	FromSeq    uint64    `db:"from_seq"`
	ToSeq      uint64    `db:"to_seq"`
	Reason     string    `db:"reason"`
	ReportedAt time.Time `db:"reported_at"`
}

// DeviceCert is one issued certificate.
type DeviceCert struct {
	Serial    string     `db:"serial"`
	TenantID  uuid.UUID  `db:"tenant_id"`
	DeviceID  uuid.UUID  `db:"device_id"`
	NotAfter  time.Time  `db:"not_after"`
	IssuedAt  time.Time  `db:"issued_at"`
	RevokedAt *time.Time `db:"revoked_at"`
}

// RevokedCert is a CRL entry.
type RevokedCert struct {
	Serial    string    `db:"serial"`
	RevokedAt time.Time `db:"revoked_at"`
}

// EnrollmentToken is a one-time device enrollment secret (hash only).
type EnrollmentToken struct {
	ID           uuid.UUID  `db:"id"`
	TenantID     uuid.UUID  `db:"tenant_id"`
	TokenHash    []byte     `db:"token_hash"`
	ExpiresAt    time.Time  `db:"expires_at"`
	UsedAt       *time.Time `db:"used_at"`
	UsedByDevice *uuid.UUID `db:"used_by_device"`
	CreatedBy    string     `db:"created_by"`
	CreatedAt    time.Time  `db:"created_at"`
}

// Enrollment outcomes from consume_enrollment_token.
const (
	EnrollOK             = "ok"
	EnrollUnknown        = "unknown"
	EnrollExpired        = "expired"
	EnrollUsed           = "used"
	EnrollTenantInactive = "tenant_inactive"
)

// APIToken is a tenant API token (hash only).
type APIToken struct {
	ID         uuid.UUID  `db:"id"`
	TenantID   uuid.UUID  `db:"tenant_id"`
	Name       string     `db:"name"`
	TokenHash  []byte     `db:"token_hash"`
	Scopes     []string   `db:"scopes"`
	CreatedAt  time.Time  `db:"created_at"`
	LastUsedAt *time.Time `db:"last_used_at"`
	RevokedAt  *time.Time `db:"revoked_at"`
}

// APITokenAuth is what authentication needs about a tenant token.
type APITokenAuth struct {
	TenantID     uuid.UUID  `db:"tenant_id"`
	TokenHash    []byte     `db:"token_hash"`
	Scopes       []string   `db:"scopes"`
	RevokedAt    *time.Time `db:"revoked_at"`
	TenantStatus string     `db:"tenant_status"`
}

// OperatorTokenAuth is what authentication needs about an operator token.
type OperatorTokenAuth struct {
	Name      string     `db:"name"`
	TokenHash []byte     `db:"token_hash"`
	RevokedAt *time.Time `db:"revoked_at"`
}

// Job kinds.
const (
	JobRetention     = "retention"
	JobExport        = "export"
	JobDevicePurge   = "device_purge"
	JobTenantDestroy = "tenant_destroy"
)

// Job states.
const (
	JobQueued    = "queued"
	JobRunning   = "running"
	JobSucceeded = "succeeded"
	JobFailed    = "failed"
)

// Job is one background job.
type Job struct {
	ID          uuid.UUID       `db:"id"`
	TenantID    uuid.UUID       `db:"tenant_id"`
	Kind        string          `db:"kind"`
	Status      string          `db:"status"`
	Payload     json.RawMessage `db:"payload"`
	Result      json.RawMessage `db:"result"`
	Attempts    int             `db:"attempts"`
	MaxAttempts int             `db:"max_attempts"`
	LastError   *string         `db:"last_error"`
	RunAfter    time.Time       `db:"run_after"`
	LockedUntil *time.Time      `db:"locked_until"`
	LockedBy    *string         `db:"locked_by"`
	DedupeKey   *string         `db:"dedupe_key"`
	CreatedAt   time.Time       `db:"created_at"`
	FinishedAt  *time.Time      `db:"finished_at"`
}

// Export states.
const (
	ExportPending = "pending"
	ExportRunning = "running"
	ExportReady   = "ready"
	ExportFailed  = "failed"
	ExportExpired = "expired"
)

// Export is one data export.
type Export struct {
	ID           uuid.UUID  `db:"id"`
	TenantID     uuid.UUID  `db:"tenant_id"`
	DeviceID     *uuid.UUID `db:"device_id"`
	Status       string     `db:"status"`
	FilePath     *string    `db:"file_path"`
	SHA256       []byte     `db:"sha256"`
	Bytes        *int64     `db:"bytes"`
	ReadingCount *int64     `db:"reading_count"`
	Error        *string    `db:"error"`
	CreatedAt    time.Time  `db:"created_at"`
	CompletedAt  *time.Time `db:"completed_at"`
	ExpiresAt    *time.Time `db:"expires_at"`
}

// Audit actor types and results.
const (
	ActorOperator = "operator"
	ActorTenant   = "tenant_token"
	ActorDevice   = "device"
	ActorSystem   = "system"

	ResultSuccess = "success"
	ResultDenied  = "denied"
	ResultFailure = "failure"
)

// AuditEntry is one row of the append-only audit log.
type AuditEntry struct {
	ID         int64           `db:"id"`
	TenantID   *uuid.UUID      `db:"tenant_id"`
	At         time.Time       `db:"at"`
	ActorType  string          `db:"actor_type"`
	ActorID    string          `db:"actor_id"`
	Action     string          `db:"action"`
	TargetType string          `db:"target_type"`
	TargetID   string          `db:"target_id"`
	Result     string          `db:"result"`
	Details    json.RawMessage `db:"details"`
}
