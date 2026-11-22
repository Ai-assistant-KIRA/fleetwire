package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tokens"
)

func (s *Server) getTenant(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	var t store.Tenant
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		t, err = tx.GetTenant(ctx)
		return err
	})
	if err != nil {
		return s.internal(r, err)
	}
	writeJSON(w, http.StatusOK, toTenant(t))
	return nil
}

func (s *Server) patchTenant(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	var req struct {
		RetentionDays *int `json:"retention_days"`
	}
	if e := decodeJSON(w, r, &req); e != nil {
		return e
	}
	if req.RetentionDays == nil || *req.RetentionDays < 1 || *req.RetentionDays > 3650 {
		return errBadRequest("retention_days must be between 1 and 3650")
	}
	var t store.Tenant
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		old, err := tx.GetTenant(ctx)
		if err != nil {
			return err
		}
		if t, err = tx.SetRetention(ctx, *req.RetentionDays); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, audit(p, "tenant.retention", "tenant", p.tenant.String(),
			map[string]any{"from": old.RetentionDays, "to": t.RetentionDays}))
	})
	if err != nil {
		return s.internal(r, err)
	}
	writeJSON(w, http.StatusOK, toTenant(t))
	return nil
}

func (s *Server) createToken(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	var req createTokenReq
	if e := decodeJSON(w, r, &req); e != nil {
		return e
	}
	out, e := s.issueToken(r, p.tenant, p, req)
	if e != nil {
		return e
	}
	writeJSON(w, http.StatusCreated, out)
	return nil
}

func (s *Server) listTokens(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	limit, e := limitParam(r, 100, 1000)
	if e != nil {
		return e
	}
	type cursor struct {
		After uuid.UUID `json:"a"`
	}
	scope := "tokens:" + p.tenant.String()
	var cur cursor
	if c := r.URL.Query().Get("cursor"); c != "" {
		if err := s.cur.decode(scope, c, &cur); err != nil {
			return errBadRequest("invalid cursor")
		}
	}
	var list []store.APIToken
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		list, err = tx.ListAPITokens(ctx, cur.After, limit)
		return err
	})
	if err != nil {
		return s.internal(r, err)
	}
	out := page[tokenJSON]{Items: make([]tokenJSON, 0, len(list))}
	for _, t := range list {
		out.Items = append(out.Items, toToken(t))
	}
	if len(list) == limit {
		c, err := s.cur.encode(scope, cursor{After: list[len(list)-1].ID})
		if err != nil {
			return s.internal(r, err)
		}
		out.NextCursor = c
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getToken(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	id, e := pathID(r)
	if e != nil {
		return e
	}
	var t store.APIToken
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		t, err = tx.GetAPIToken(ctx, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("token")
	}
	if err != nil {
		return s.internal(r, err)
	}
	writeJSON(w, http.StatusOK, toToken(t))
	return nil
}

func (s *Server) revokeToken(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	id, e := pathID(r)
	if e != nil {
		return e
	}
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.GetAPIToken(ctx, id); err != nil {
			return err
		}
		changed, err := tx.RevokeAPIToken(ctx, id)
		if err != nil || !changed {
			return err
		}
		return tx.InsertAudit(ctx, audit(p, "token.revoke", "api_token", id.String(), nil))
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("token")
	}
	if err != nil {
		return s.internal(r, err)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) createEnrollmentTokens(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	return s.issueEnrollmentTokens(w, r, p.tenant, p)
}

func (s *Server) listDevices(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	limit, e := limitParam(r, 100, 1000)
	if e != nil {
		return e
	}
	status := r.URL.Query().Get("status")
	switch status {
	case "", store.DeviceActive, store.DeviceRevoked, store.DeviceDeleted:
	default:
		return errBadRequest("status must be active, revoked or deleted")
	}
	type cursor struct {
		After uuid.UUID `json:"a"`
	}
	scope := "devices:" + p.tenant.String() + ":" + status
	var cur cursor
	if c := r.URL.Query().Get("cursor"); c != "" {
		if err := s.cur.decode(scope, c, &cur); err != nil {
			return errBadRequest("invalid cursor")
		}
	}
	var list []store.Device
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		list, err = tx.ListDevices(ctx, status, cur.After, limit)
		return err
	})
	if err != nil {
		return s.internal(r, err)
	}
	out := page[deviceJSON]{Items: make([]deviceJSON, 0, len(list))}
	for _, d := range list {
		out.Items = append(out.Items, toDevice(d))
	}
	if len(list) == limit {
		c, err := s.cur.encode(scope, cursor{After: list[len(list)-1].ID})
		if err != nil {
			return s.internal(r, err)
		}
		out.NextCursor = c
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getDevice(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	id, e := pathID(r)
	if e != nil {
		return e
	}
	var out deviceJSON
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		d, err := tx.GetDevice(ctx, id)
		if err != nil {
			return err
		}
		n, seqs, err := tx.CountGaps(ctx, id)
		if err != nil {
			return err
		}
		out = toDevice(d)
		out.GapCount, out.GapSeqs = &n, &seqs
		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("device")
	}
	if err != nil {
		return s.internal(r, err)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) tenantRevokeDevice(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	id, e := pathID(r)
	if e != nil {
		return e
	}
	var dev store.Device
	var changed bool
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
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

// deleteDevice revokes the device first, so messages still in the pipeline
// are dropped by ingest instead of stored again, then queues the purge.
func (s *Server) deleteDevice(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	id, e := pathID(r)
	if e != nil {
		return e
	}
	jobID, err := uuid.NewV7()
	if err != nil {
		return s.internal(r, err)
	}
	var apiErr *apiError
	err = s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		d, _, err := revokeDevice(ctx, tx, id, p)
		if err != nil {
			return err
		}
		if d.Status == store.DeviceDeleted {
			apiErr = errNotFound("device")
			return nil
		}
		// When a purge job already exists, this is its ID.
		if jobID, err = tx.EnqueueJob(ctx, jobID, store.JobDevicePurge, map[string]any{"device_id": id},
			"device_purge:"+id.String(), s.now()); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, audit(p, "device.delete", "device", id.String(), nil))
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("device")
	}
	if err != nil {
		return s.internal(r, err)
	}
	if apiErr != nil {
		return apiErr
	}
	s.crl.Trigger()
	writeJSON(w, http.StatusAccepted, acceptedJSON{JobID: jobID, StatusURL: "/v1/jobs/" + jobID.String()})
	return nil
}

func (s *Server) listAudit(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	return s.auditPage(w, r, "audit", func(ctx context.Context, tx *store.Tx, before int64, limit int) ([]store.AuditEntry, error) {
		return tx.ListAudit(ctx, before, limit)
	}, p.tenant)
}

// jobScopes is the scope needed to read a job of each kind: the read scope
// of what the job works on. Kinds missing here are refused.
var jobScopes = map[string]string{
	store.JobExport:        tokens.ScopeExportsRead,
	store.JobDevicePurge:   tokens.ScopeDevicesRead,
	store.JobRetention:     tokens.ScopeTenantAdmin,
	store.JobTenantDestroy: tokens.ScopeTenantAdmin,
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	id, e := pathID(r)
	if e != nil {
		return e
	}
	var j store.Job
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		j, err = tx.GetJob(ctx, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("job")
	}
	if err != nil {
		return s.internal(r, err)
	}
	if scope, ok := jobScopes[j.Kind]; !ok || !p.has(scope) {
		return errForbidden
	}
	writeJSON(w, http.StatusOK, toJob(j))
	return nil
}
