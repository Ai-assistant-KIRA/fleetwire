package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tokens"
)

func audit(p *principal, action, targetType, targetID string, details map[string]any) store.Audit {
	return store.Audit{ActorType: p.actorType(), ActorID: p.actorID(), Action: action,
		TargetType: targetType, TargetID: targetID, Result: store.ResultSuccess, Details: details}
}

// nameRule is the error text for names validName refuses.
const nameRule = "name must be 1 to 200 characters, without control characters"

// validName accepts 1 to 200 characters after trimming spaces. Control
// characters are refused: PostgreSQL text cannot hold NUL, and the others
// break logs and terminals that show the name.
func validName(s string) bool {
	if !utf8.ValidString(s) || strings.ContainsFunc(s, unicode.IsControl) {
		return false
	}
	n := utf8.RuneCountInString(strings.TrimSpace(s))
	return n >= 1 && n <= 200
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	var req struct {
		Name          string `json:"name"`
		RetentionDays *int   `json:"retention_days"`
	}
	if e := decodeJSON(w, r, &req); e != nil {
		return e
	}
	days := 30
	if req.RetentionDays != nil {
		days = *req.RetentionDays
	}
	var problems []string
	if !validName(req.Name) {
		problems = append(problems, nameRule)
	}
	if days < 1 || days > 3650 {
		problems = append(problems, "retention_days must be between 1 and 3650")
	}
	if len(problems) > 0 {
		return errBadRequest("invalid tenant", problems...)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return s.internal(r, err)
	}
	// The KEK lives outside the database (ADR-0007). If the database
	// transaction fails, the orphaned KEK is destroyed again.
	if err := s.kr.Create(r.Context(), id); err != nil {
		return s.internal(r, err)
	}
	var t store.Tenant
	err = s.db.InTenantTx(r.Context(), id, func(ctx context.Context, tx *store.Tx) error {
		var err error
		if t, err = tx.CreateTenant(ctx, strings.TrimSpace(req.Name), days); err != nil {
			return err
		}
		if err := s.keys.CreateFirstKey(ctx, tx); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, audit(p, "tenant.create", "tenant", id.String(),
			map[string]any{"name": t.Name, "retention_days": days}))
	})
	if err != nil {
		if derr := s.kr.Destroy(context.WithoutCancel(r.Context()), id); derr != nil {
			s.log.Error("could not remove KEK of a tenant that was not created", "tenant_id", id, "err", derr)
		}
		return s.internal(r, err)
	}
	writeJSON(w, http.StatusCreated, toTenant(t))
	return nil
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request, _ *principal) *apiError {
	limit, e := limitParam(r, 100, 1000)
	if e != nil {
		return e
	}
	var cur struct {
		After uuid.UUID `json:"a"`
	}
	if c := r.URL.Query().Get("cursor"); c != "" {
		if err := s.cur.decode("admin.tenants", c, &cur); err != nil {
			return errBadRequest("invalid cursor")
		}
	}
	var list []store.Tenant
	err := s.db.InTx(r.Context(), func(ctx context.Context, tx *store.Tx) error {
		var err error
		list, err = tx.ListTenants(ctx, cur.After, limit+1)
		return err
	})
	if err != nil {
		return s.internal(r, err)
	}
	list, more := trimPage(list, limit)
	out := page[tenantJSON]{Items: make([]tenantJSON, 0, len(list))}
	for _, t := range list {
		out.Items = append(out.Items, toTenant(t))
	}
	if more {
		c, err := s.cur.encode("admin.tenants", struct {
			After uuid.UUID `json:"a"`
		}{list[len(list)-1].ID})
		if err != nil {
			return s.internal(r, err)
		}
		out.NextCursor = c
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// destroyTenant runs the first destroy step synchronously, so the tenant is
// unusable when the call returns, and leaves the rest to a restartable job
// (design 13.4).
func (s *Server) destroyTenant(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	id, e := pathID(r)
	if e != nil {
		return e
	}
	jobID, err := uuid.NewV7()
	if err != nil {
		return s.internal(r, err)
	}
	var apiErr *apiError
	err = s.db.InTenantTx(r.Context(), id, func(ctx context.Context, tx *store.Tx) error {
		t, err := tx.LockTenant(ctx)
		if errors.Is(err, store.ErrNotFound) {
			apiErr = errNotFound("tenant")
			return nil
		}
		if err != nil {
			return err
		}
		if t.Status == store.TenantDestroyed {
			apiErr = errConflict("tenant is already destroyed")
			return nil
		}
		if t.Status == store.TenantActive {
			if err := tx.SetTenantStatus(ctx, store.TenantDestroying); err != nil {
				return err
			}
			if _, err := tx.RevokeAllDevices(ctx); err != nil {
				return err
			}
			if err := tx.DestroyTenantKeys(ctx); err != nil {
				return err
			}
		}
		// When a destroy job already exists, this is its ID.
		if jobID, err = tx.EnqueueJob(ctx, jobID, store.JobTenantDestroy, map[string]any{"tenant_id": id},
			"tenant_destroy:"+id.String(), s.now()); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, audit(p, "tenant.destroy", "tenant", id.String(), nil))
	})
	if err != nil {
		return s.internal(r, err)
	}
	if apiErr != nil {
		return apiErr
	}
	s.keys.Forget(id)
	// Step 2 right away; the job repeats it if this fails.
	if err := s.kr.Destroy(context.WithoutCancel(r.Context()), id); err != nil {
		s.log.Error("tenant key destroy failed; the destroy job will retry", "tenant_id", id, "err", err)
	}
	s.crl.Trigger()
	// The tenant's own tokens stop working at once, so progress is visible
	// through the operator's tenant list (status destroying, then destroyed).
	writeJSON(w, http.StatusAccepted, acceptedJSON{JobID: jobID})
	return nil
}

type createTokenReq struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

func (s *Server) issueToken(r *http.Request, tenant uuid.UUID, p *principal, req createTokenReq) (tokenJSON, *apiError) {
	var problems []string
	if !validName(req.Name) {
		problems = append(problems, nameRule)
	}
	if err := tokens.ValidateScopes(req.Scopes); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return tokenJSON{}, errBadRequest("invalid token request", problems...)
	}
	id, secret, hash, err := tokens.NewKeyed(tokens.PrefixTenant)
	if err != nil {
		return tokenJSON{}, s.internal(r, err)
	}
	var out tokenJSON
	var apiErr *apiError
	err = s.db.InTenantTx(r.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		t, err := tx.GetTenant(ctx)
		if errors.Is(err, store.ErrNotFound) {
			apiErr = errNotFound("tenant")
			return nil
		}
		if err != nil {
			return err
		}
		if t.Status != store.TenantActive {
			apiErr = errConflict("tenant is not active")
			return nil
		}
		tok, err := tx.InsertAPIToken(ctx, id, strings.TrimSpace(req.Name), hash, req.Scopes)
		if err != nil {
			return err
		}
		out = toToken(tok)
		return tx.InsertAudit(ctx, audit(p, "token.create", "api_token", id.String(),
			map[string]any{"name": tok.Name, "scopes": tok.Scopes}))
	})
	if err != nil {
		return tokenJSON{}, s.internal(r, err)
	}
	if apiErr != nil {
		return tokenJSON{}, apiErr
	}
	out.Token = secret.Reveal()
	return out, nil
}

func (s *Server) adminCreateToken(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	tenant, e := pathID(r)
	if e != nil {
		return e
	}
	var req createTokenReq
	if e := decodeJSON(w, r, &req); e != nil {
		return e
	}
	out, e := s.issueToken(r, tenant, p, req)
	if e != nil {
		return e
	}
	writeJSON(w, http.StatusCreated, out)
	return nil
}

// enrollReq has pointers so that a field left out takes its default while
// an explicit 0 is refused, as the schema says.
type enrollReq struct {
	Count      *int `json:"count"`
	TTLSeconds *int `json:"ttl_seconds"`
}

func (s *Server) issueEnrollmentTokens(w http.ResponseWriter, r *http.Request, tenant uuid.UUID, p *principal) *apiError {
	var req enrollReq
	if e := decodeOptionalJSON(w, r, &req); e != nil {
		return e
	}
	count, ttl, ttlOK := 1, s.cfg.EnrollTokenTTL, true
	if req.Count != nil {
		count = *req.Count
	}
	// Checked in seconds: a large value would overflow time.Duration.
	if req.TTLSeconds != nil {
		ttlOK = *req.TTLSeconds >= 60 && *req.TTLSeconds <= 86400
		ttl = time.Duration(*req.TTLSeconds) * time.Second
	}
	if !ttlOK || count < 1 || count > 1000 || ttl < time.Minute || ttl > 24*time.Hour {
		return errBadRequest("count must be 1 to 1000 and ttl_seconds 60 to 86400")
	}
	expires := s.now().Add(ttl).UTC().Truncate(time.Second)
	out := struct {
		Tokens []enrollmentTokenJSON `json:"tokens"`
	}{}
	var apiErr *apiError
	err := s.db.InTenantTx(r.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		t, err := tx.GetTenant(ctx)
		if errors.Is(err, store.ErrNotFound) {
			apiErr = errNotFound("tenant")
			return nil
		}
		if err != nil {
			return err
		}
		if t.Status != store.TenantActive {
			apiErr = errConflict("tenant is not active")
			return nil
		}
		for i := 0; i < count; i++ {
			secret, hash, err := tokens.NewEnrollment()
			if err != nil {
				return err
			}
			if err := tx.InsertEnrollmentToken(ctx, uuid.New(), hash, expires, p.actorType()+":"+p.actorID()); err != nil {
				return err
			}
			out.Tokens = append(out.Tokens, enrollmentTokenJSON{Token: secret.Reveal(), ExpiresAt: expires})
		}
		return tx.InsertAudit(ctx, audit(p, "enrollment_token.create", "tenant", tenant.String(),
			map[string]any{"count": count, "expires_at": expires}))
	})
	if err != nil {
		return s.internal(r, err)
	}
	if apiErr != nil {
		return apiErr
	}
	writeJSON(w, http.StatusCreated, out)
	return nil
}

func (s *Server) adminEnrollmentTokens(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	tenant, e := pathID(r)
	if e != nil {
		return e
	}
	return s.issueEnrollmentTokens(w, r, tenant, p)
}

// revokeDevice revokes a device inside tx and audits it. Ingest drops the
// device's data from the commit on; the broker refuses it after the next
// CRL reload (ADR-0010).
func revokeDevice(ctx context.Context, tx *store.Tx, id uuid.UUID, p *principal) (store.Device, bool, error) {
	d, err := tx.GetDevice(ctx, id)
	if err != nil {
		return store.Device{}, false, err
	}
	changed, err := tx.RevokeDevice(ctx, id)
	if err != nil {
		return store.Device{}, false, err
	}
	if changed {
		if err := tx.InsertAudit(ctx, audit(p, "device.revoke", "device", id.String(), nil)); err != nil {
			return store.Device{}, false, err
		}
		if d, err = tx.GetDevice(ctx, id); err != nil {
			return store.Device{}, false, err
		}
	}
	return d, changed, nil
}

func (s *Server) adminRevokeDevice(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	id, e := pathID(r)
	if e != nil {
		return e
	}
	var dev store.Device
	var changed bool
	err := s.db.InTx(r.Context(), func(ctx context.Context, tx *store.Tx) error {
		ref, err := tx.DeviceLookup(ctx, id)
		if err != nil {
			return err
		}
		if err := tx.SetTenant(ctx, ref.TenantID); err != nil {
			return err
		}
		dev, changed, err = revokeDevice(ctx, tx, id, p)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("device")
	}
	if err != nil {
		return s.internal(r, err)
	}
	if changed {
		s.crl.Trigger()
	}
	writeJSON(w, http.StatusOK, toDevice(dev))
	return nil
}

func (s *Server) adminAudit(w http.ResponseWriter, r *http.Request, _ *principal) *apiError {
	return s.auditPage(w, r, "admin.audit", func(ctx context.Context, tx *store.Tx, before int64, limit int) ([]store.AuditEntry, error) {
		return tx.OperatorAudit(ctx, before, limit)
	}, uuid.Nil)
}

type auditLister func(ctx context.Context, tx *store.Tx, before int64, limit int) ([]store.AuditEntry, error)

func (s *Server) auditPage(w http.ResponseWriter, r *http.Request, scope string, list auditLister, tenant uuid.UUID) *apiError {
	limit, e := limitParam(r, 100, 1000)
	if e != nil {
		return e
	}
	type cursor struct {
		Before int64 `json:"b"`
	}
	scope += ":" + tenant.String()
	var cur cursor
	if c := r.URL.Query().Get("cursor"); c != "" {
		if err := s.cur.decode(scope, c, &cur); err != nil {
			return errBadRequest("invalid cursor")
		}
	}
	var entries []store.AuditEntry
	run := func(ctx context.Context, tx *store.Tx) error {
		var err error
		entries, err = list(ctx, tx, cur.Before, limit+1)
		return err
	}
	var err error
	if tenant == uuid.Nil {
		err = s.db.InTx(r.Context(), run)
	} else {
		err = s.db.InTenantTx(r.Context(), tenant, run)
	}
	if err != nil {
		return s.internal(r, err)
	}
	entries, more := trimPage(entries, limit)
	out := page[auditJSON]{Items: make([]auditJSON, 0, len(entries))}
	for _, a := range entries {
		out.Items = append(out.Items, toAudit(a))
	}
	if more {
		c, err := s.cur.encode(scope, cursor{Before: entries[len(entries)-1].ID})
		if err != nil {
			return s.internal(r, err)
		}
		out.NextCursor = c
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}
