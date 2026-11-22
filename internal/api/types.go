package api

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

type page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type tenantJSON struct {
	ID            uuid.UUID  `json:"id"`
	Name          string     `json:"name"`
	Status        string     `json:"status"`
	RetentionDays int        `json:"retention_days"`
	CreatedAt     time.Time  `json:"created_at"`
	DestroyedAt   *time.Time `json:"destroyed_at,omitempty"`
}

func toTenant(t store.Tenant) tenantJSON {
	return tenantJSON{ID: t.ID, Name: t.Name, Status: t.Status, RetentionDays: t.RetentionDays,
		CreatedAt: t.CreatedAt, DestroyedAt: t.DestroyedAt}
}

type deviceJSON struct {
	ID           uuid.UUID  `json:"id"`
	Name         string     `json:"name"`
	Status       string     `json:"status"`
	AckedSeq     uint64     `json:"acked_seq"`
	LastSeenAt   *time.Time `json:"last_seen_at,omitempty"`
	LastBatchAt  *time.Time `json:"last_batch_at,omitempty"`
	CertNotAfter *time.Time `json:"cert_not_after,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
	GapCount     *int64     `json:"gap_count,omitempty"`
	GapSeqs      *int64     `json:"gap_seqs,omitempty"`
}

func toDevice(d store.Device) deviceJSON {
	return deviceJSON{ID: d.ID, Name: d.Name, Status: d.Status, AckedSeq: d.AckedThrough, LastSeenAt: d.LastSeenAt,
		LastBatchAt: d.LastBatchAt, CertNotAfter: d.CertNotAfter, CreatedAt: d.CreatedAt, RevokedAt: d.RevokedAt}
}

type tokenJSON struct {
	ID         uuid.UUID  `json:"id"`
	Name       string     `json:"name"`
	Scopes     []string   `json:"scopes"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	// Token is returned once, when the token is created.
	Token string `json:"token,omitempty"`
}

func toToken(t store.APIToken) tokenJSON {
	return tokenJSON{ID: t.ID, Name: t.Name, Scopes: t.Scopes, CreatedAt: t.CreatedAt,
		LastUsedAt: t.LastUsedAt, RevokedAt: t.RevokedAt}
}

type enrollmentTokenJSON struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type readingJSON struct {
	Seq         uint64            `json:"seq"`
	CollectedAt time.Time         `json:"collected_at"`
	Collector   string            `json:"collector"`
	Metric      string            `json:"metric"`
	Value       float64           `json:"value"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type gapJSON struct {
	FromSeq    uint64    `json:"from_seq"`
	ToSeq      uint64    `json:"to_seq"`
	Reason     string    `json:"reason"`
	ReportedAt time.Time `json:"reported_at"`
}

type exportJSON struct {
	ID           uuid.UUID  `json:"id"`
	DeviceID     *uuid.UUID `json:"device_id,omitempty"`
	Status       string     `json:"status"`
	Bytes        *int64     `json:"bytes,omitempty"`
	ReadingCount *int64     `json:"reading_count,omitempty"`
	SHA256       string     `json:"sha256,omitempty"`
	Error        *string    `json:"error,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
	DownloadURL  string     `json:"download_url,omitempty"`
}

func toExport(e store.Export) exportJSON {
	out := exportJSON{ID: e.ID, DeviceID: e.DeviceID, Status: e.Status, Bytes: e.Bytes, ReadingCount: e.ReadingCount,
		Error: e.Error, CreatedAt: e.CreatedAt, CompletedAt: e.CompletedAt, ExpiresAt: e.ExpiresAt}
	if len(e.SHA256) > 0 {
		out.SHA256 = hex.EncodeToString(e.SHA256)
	}
	if e.Status == store.ExportReady {
		out.DownloadURL = "/v1/exports/" + e.ID.String() + "/download"
	}
	return out
}

type auditJSON struct {
	ID         int64           `json:"id"`
	TenantID   *uuid.UUID      `json:"tenant_id,omitempty"`
	At         time.Time       `json:"at"`
	ActorType  string          `json:"actor_type"`
	ActorID    string          `json:"actor_id"`
	Action     string          `json:"action"`
	TargetType string          `json:"target_type"`
	TargetID   string          `json:"target_id"`
	Result     string          `json:"result"`
	Details    json.RawMessage `json:"details"`
}

func toAudit(a store.AuditEntry) auditJSON {
	return auditJSON{ID: a.ID, TenantID: a.TenantID, At: a.At, ActorType: a.ActorType, ActorID: a.ActorID,
		Action: a.Action, TargetType: a.TargetType, TargetID: a.TargetID, Result: a.Result, Details: a.Details}
}

type jobJSON struct {
	ID         uuid.UUID       `json:"id"`
	Kind       string          `json:"kind"`
	Status     string          `json:"status"`
	Attempts   int             `json:"attempts"`
	LastError  *string         `json:"last_error,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
}

func toJob(j store.Job) jobJSON {
	return jobJSON{ID: j.ID, Kind: j.Kind, Status: j.Status, Attempts: j.Attempts, LastError: j.LastError,
		Result: j.Result, CreatedAt: j.CreatedAt, FinishedAt: j.FinishedAt}
}

type acceptedJSON struct {
	// JobID is the job doing the work: the one just queued, or an identical
	// job that was already queued, running or done.
	JobID     uuid.UUID `json:"job_id"`
	StatusURL string    `json:"status_url,omitempty"`
}

// limitParam reads ?limit= with a default and a maximum.
func limitParam(r *http.Request, def, maxN int) (int, *apiError) {
	s := r.URL.Query().Get("limit")
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > maxN {
		return 0, errBadRequest("limit must be between 1 and " + strconv.Itoa(maxN))
	}
	return n, nil
}
