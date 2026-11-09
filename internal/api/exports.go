package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

func (s *Server) createExport(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	var req struct {
		DeviceID *uuid.UUID `json:"device_id"`
	}
	if r.ContentLength != 0 {
		if e := decodeJSON(w, r, &req); e != nil {
			return e
		}
	}
	id, err := uuid.NewV7()
	if err != nil {
		return s.internal(r, err)
	}
	var exp store.Export
	err = s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		if req.DeviceID != nil {
			if _, err := tx.GetDevice(ctx, *req.DeviceID); err != nil {
				return err
			}
		}
		var err error
		if exp, err = tx.InsertExport(ctx, id, req.DeviceID); err != nil {
			return err
		}
		if _, err := tx.EnqueueJob(ctx, uuid.Must(uuid.NewV7()), store.JobExport, map[string]any{"export_id": id}, "export:"+id.String(), s.now()); err != nil {
			return err
		}
		details := map[string]any{}
		if req.DeviceID != nil {
			details["device_id"] = req.DeviceID.String()
		}
		return tx.InsertAudit(ctx, audit(p, "export.create", "export", id.String(), details))
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("device")
	}
	if err != nil {
		return s.internal(r, err)
	}
	w.Header().Set("Location", "/v1/exports/"+id.String())
	writeJSON(w, http.StatusAccepted, toExport(exp))
	return nil
}

func (s *Server) loadExport(r *http.Request, p *principal) (store.Export, *apiError) {
	id, e := pathID(r)
	if e != nil {
		return store.Export{}, e
	}
	var exp store.Export
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		exp, err = tx.GetExport(ctx, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return store.Export{}, errNotFound("export")
	}
	if err != nil {
		return store.Export{}, s.internal(r, err)
	}
	return exp, nil
}

func (s *Server) getExport(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	exp, e := s.loadExport(r, p)
	if e != nil {
		return e
	}
	writeJSON(w, http.StatusOK, toExport(exp))
	return nil
}

// downloadExport streams the gzip file with an RFC 9530 Content-Digest.
func (s *Server) downloadExport(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	exp, e := s.loadExport(r, p)
	if e != nil {
		return e
	}
	switch exp.Status {
	case store.ExportReady:
	case store.ExportExpired:
		return &apiError{status: http.StatusGone, code: "expired", detail: "the export expired; create a new one"}
	default:
		return errConflict("the export is " + exp.Status)
	}
	if exp.FilePath == nil {
		return s.internal(r, errors.New("ready export without a file"))
	}
	// The path comes from the database, but it must still be inside the
	// export directory.
	path := filepath.Clean(*exp.FilePath)
	if rel, err := filepath.Rel(s.cfg.ExportDir, path); err != nil || strings.HasPrefix(rel, "..") {
		return s.internal(r, fmt.Errorf("export path %q outside %q", path, s.cfg.ExportDir))
	}
	f, err := os.Open(path)
	if err != nil {
		return s.internal(r, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return s.internal(r, err)
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Hour)); err != nil {
		s.log.Warn("cannot extend write deadline for export download", "err", err)
	}
	h := w.Header()
	h.Set("Content-Type", "application/gzip")
	h.Set("Content-Disposition", `attachment; filename="fleetwire-export-`+exp.ID.String()+`.ndjson.gz"`)
	h.Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	h.Set("Content-Digest", "sha-256=:"+base64.StdEncoding.EncodeToString(exp.SHA256)+":")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, f); err != nil {
		s.log.Warn("export download interrupted", "export_id", exp.ID, "err", err)
	}
	return nil
}
