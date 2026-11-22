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
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

func (s *Server) createExport(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	var req struct {
		DeviceID *uuid.UUID `json:"device_id"`
	}
	if e := decodeOptionalJSON(w, r, &req); e != nil {
		return e
	}
	id, err := uuid.NewV7()
	if err != nil {
		return s.internal(r, err)
	}
	var exp store.Export
	var apiErr *apiError
	err = s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		if req.DeviceID != nil {
			if _, err := tx.GetDevice(ctx, *req.DeviceID); err != nil {
				return err
			}
		}
		// Each export holds a worker and a file until it finishes, so the
		// number a tenant can queue is capped.
		active, err := tx.LockActiveExports(ctx)
		if err != nil {
			return err
		}
		if active >= s.cfg.MaxActiveExports {
			apiErr = &apiError{status: http.StatusConflict, code: "too_many_exports",
				detail: fmt.Sprintf("%d exports are already pending or running; wait for one to finish", active)}
			return nil
		}
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
	if apiErr != nil {
		return apiErr
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
	expired := &apiError{status: http.StatusGone, code: "expired", detail: "the export expired; create a new one"}
	switch exp.Status {
	case store.ExportReady:
	case store.ExportExpired:
		return expired
	default:
		return errConflict("the export is " + exp.Status)
	}
	// Retention removes expired files only when it runs; until then the
	// file is still there but no longer offered.
	if exp.ExpiresAt != nil && !s.now().Before(*exp.ExpiresAt) {
		return expired
	}
	if exp.FilePath == nil {
		return s.internal(r, errors.New("ready export without a file"))
	}
	f, err := s.openExport(*exp.FilePath)
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
	// GET routes also answer HEAD, which sends no body: skip reading the file.
	if r.Method == http.MethodHead {
		return nil
	}
	if _, err := io.Copy(w, f); err != nil {
		s.log.Warn("export download interrupted", "export_id", exp.ID, "err", err)
	}
	return nil
}

// openExport opens an export file through an os.Root on the export
// directory. The path comes from the database, but a path that leaves the
// directory, by ".." or through a symbolic link, is still refused.
func (s *Server) openExport(path string) (*os.File, error) {
	rel, err := filepath.Rel(s.cfg.ExportDir, path)
	if err != nil {
		return nil, fmt.Errorf("export path %q: %w", path, err)
	}
	root, err := os.OpenRoot(s.cfg.ExportDir)
	if err != nil {
		return nil, fmt.Errorf("open export directory: %w", err)
	}
	// Files opened through the root stay open after it closes.
	defer root.Close()
	f, err := root.Open(rel)
	if err != nil {
		return nil, fmt.Errorf("open export file: %w", err)
	}
	return f, nil
}
