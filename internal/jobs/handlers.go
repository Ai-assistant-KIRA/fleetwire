package jobs

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

type retentionResult struct {
	Cutoff         time.Time `json:"cutoff"`
	ChunksDeleted  int64     `json:"chunks_deleted"`
	GapsDeleted    int64     `json:"gaps_deleted"`
	ExportsExpired int       `json:"exports_expired"`
}

// retention deletes readings and gap records older than the tenant's
// retention period, and export files past their expiry.
func (w *Worker) retention(ctx context.Context, tenant uuid.UUID) (any, error) {
	var t store.Tenant
	err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		t, err = tx.GetTenant(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	res := retentionResult{Cutoff: w.now().UTC().Add(-time.Duration(t.RetentionDays) * 24 * time.Hour)}
	if t.Status != store.TenantActive {
		return res, nil
	}
	if res.ChunksDeleted, err = w.deleteInBatches(ctx, tenant, func(ctx context.Context, tx *store.Tx) (int64, error) {
		return tx.DeleteChunksBefore(ctx, res.Cutoff, w.cfg.BatchSize)
	}); err != nil {
		return nil, err
	}
	if res.GapsDeleted, err = w.deleteInBatches(ctx, tenant, func(ctx context.Context, tx *store.Tx) (int64, error) {
		return tx.DeleteGapsBefore(ctx, res.Cutoff, w.cfg.BatchSize)
	}); err != nil {
		return nil, err
	}
	for {
		var expired []store.Export
		err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
			var err error
			expired, err = tx.ExpiredExports(ctx, w.now(), 100)
			return err
		})
		if err != nil {
			return nil, err
		}
		if len(expired) == 0 {
			return res, nil
		}
		for _, e := range expired {
			if e.FilePath != nil {
				if err := os.Remove(*e.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
					return nil, fmt.Errorf("remove expired export: %w", err)
				}
			}
			if err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
				return tx.MarkExportExpired(ctx, e.ID)
			}); err != nil {
				return nil, err
			}
			res.ExportsExpired++
		}
	}
}

type exportLine struct {
	DeviceID    uuid.UUID         `json:"device_id"`
	Seq         uint64            `json:"seq"`
	CollectedAt time.Time         `json:"collected_at"`
	Collector   string            `json:"collector"`
	Metric      string            `json:"metric"`
	Value       float64           `json:"value"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type exportResult struct {
	Readings int64  `json:"readings"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256"`
}

// export writes the tenant's (or one device's) readings as gzip NDJSON,
// hashing the file while writing it. A retry starts over from a fresh temp
// file, so a crash never leaves a half-written export marked ready.
func (w *Worker) export(ctx context.Context, tenant, id uuid.UUID) (any, error) {
	var exp store.Export
	err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		if exp, err = tx.GetExport(ctx, id); err != nil {
			return err
		}
		if exp.Status == store.ExportPending {
			return tx.SetExportRunning(ctx, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if exp.Status == store.ExportReady || exp.Status == store.ExportExpired {
		return exportResult{}, nil
	}

	dir := filepath.Join(w.cfg.ExportDir, tenant.String())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("export dir: %w", err)
	}
	final := filepath.Join(dir, id.String()+".ndjson.gz")
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create export file: %w", err)
	}
	defer func() { _ = os.Remove(tmp) }()
	hash := sha256.New()
	counter := &countWriter{w: io.MultiWriter(f, hash)}
	bw := bufio.NewWriterSize(counter, 256<<10)
	gz := gzip.NewWriter(bw)
	enc := json.NewEncoder(gz)

	devices, err := w.exportDevices(ctx, tenant, exp.DeviceID)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	var readings int64
	for _, dev := range devices {
		n, err := w.exportDevice(ctx, tenant, dev, enc)
		readings += n
		if err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	if err := gz.Close(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("close gzip: %w", err)
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("flush export: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fsync export: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("close export: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return nil, fmt.Errorf("rename export: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	sum := hash.Sum(nil)
	err = w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		return tx.CompleteExport(ctx, id, final, sum, counter.n, readings, w.now().Add(w.cfg.ExportTTL))
	})
	if err != nil {
		return nil, err
	}
	return exportResult{Readings: readings, Bytes: counter.n, SHA256: fmt.Sprintf("%x", sum)}, nil
}

func (w *Worker) exportDevices(ctx context.Context, tenant uuid.UUID, only *uuid.UUID) ([]uuid.UUID, error) {
	if only != nil {
		return []uuid.UUID{*only}, nil
	}
	var out []uuid.UUID
	after := uuid.Nil
	for {
		var list []store.Device
		err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
			var err error
			list, err = tx.ListDevices(ctx, "", after, 1000)
			return err
		})
		if err != nil {
			return nil, err
		}
		for _, d := range list {
			out = append(out, d.ID)
		}
		if len(list) < 1000 {
			return out, nil
		}
		after = list[len(list)-1].ID
	}
}

func (w *Worker) exportDevice(ctx context.Context, tenant, device uuid.UUID, enc *json.Encoder) (int64, error) {
	var n int64
	var after uint64
	for {
		var list []store.Chunk
		err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
			var err error
			list, err = tx.ChunksBySeq(ctx, device, after, 200)
			return err
		})
		if err != nil {
			return n, err
		}
		for _, c := range list {
			dek, err := w.d.Keys.Key(ctx, tenant, c.KeyID)
			if err != nil {
				return n, err
			}
			readings, err := chunks.Open(dek, c)
			if err != nil {
				return n, err
			}
			for _, r := range readings {
				if err := enc.Encode(exportLine{DeviceID: device, Seq: r.GetSeq(),
					CollectedAt: time.Unix(0, r.GetCollectedUnixNano()).UTC(), Collector: r.GetCollector(),
					Metric: r.GetMetric(), Value: r.GetValue(), Labels: r.GetLabels()}); err != nil {
					return n, fmt.Errorf("write export line: %w", err)
				}
				n++
			}
			after = c.SeqLo
		}
		if len(list) < 200 {
			return n, nil
		}
	}
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}
	return nil
}

type purgeResult struct {
	ChunksDeleted int64 `json:"chunks_deleted"`
}

// purgeDevice deletes every reading, gap and range of a revoked device, then
// marks it deleted.
func (w *Worker) purgeDevice(ctx context.Context, tenant, device uuid.UUID) (any, error) {
	n, err := w.deleteInBatches(ctx, tenant, func(ctx context.Context, tx *store.Tx) (int64, error) {
		return tx.DeleteDeviceData(ctx, device, w.cfg.BatchSize)
	})
	if err != nil {
		return nil, err
	}
	err = w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.MarkDeviceDeleted(ctx, device); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, store.Audit{ActorType: store.ActorSystem, ActorID: "jobs", Action: "device.purged",
			TargetType: "device", TargetID: device.String(), Result: store.ResultSuccess,
			Details: map[string]any{"chunks_deleted": n}})
	})
	if err != nil {
		return nil, err
	}
	return purgeResult{ChunksDeleted: n}, nil
}

type destroyResult struct {
	RowsDeleted int64 `json:"rows_deleted"`
	FilesGone   int   `json:"export_files_deleted"`
}

// destroyTenant finishes a tenant destroy (design 13.4). Each step checks
// the current state, so a crashed attempt continues where it stopped:
//  1. tenant destroying, devices revoked, wrapped keys gone (normally done
//     by the API call already);
//  2. the tenant's KEK destroyed: from here on no copy of its data,
//     backups included, can be decrypted;
//  3. rows and export files deleted in batches;
//  4. tenant marked destroyed; the tenant row and audit log stay.
func (w *Worker) destroyTenant(ctx context.Context, tenant uuid.UUID) (any, error) {
	var t store.Tenant
	err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		if t, err = tx.LockTenant(ctx); err != nil {
			return err
		}
		if t.Status == store.TenantDestroyed {
			return nil
		}
		if t.Status == store.TenantActive {
			if err := tx.SetTenantStatus(ctx, store.TenantDestroying); err != nil {
				return err
			}
		}
		// Revoked on every attempt, not only when this job starts the
		// destroy: a device enrolled or renewed past an earlier first step
		// (before enrollment and renewal locked the tenant) would otherwise
		// keep a valid certificate. It is a no-op when nothing is active.
		if _, err := tx.RevokeAllDevices(ctx); err != nil {
			return err
		}
		return tx.DestroyTenantKeys(ctx)
	})
	if err != nil {
		return nil, err
	}
	if t.Status == store.TenantDestroyed {
		return destroyResult{}, nil
	}
	if err := w.d.Keyring.Destroy(ctx, tenant); err != nil {
		return nil, err
	}
	w.d.Keys.Forget(tenant)
	w.d.CRL.Trigger()

	var res destroyResult
	var files []string
	err = w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		files, err = tx.ExportFiles(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("remove export file: %w", err)
		}
		res.FilesGone++
	}
	if res.RowsDeleted, err = w.deleteInBatches(ctx, tenant, func(ctx context.Context, tx *store.Tx) (int64, error) {
		return tx.DeleteTenantData(ctx, w.cfg.BatchSize)
	}); err != nil {
		return nil, err
	}
	err = w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.SetTenantStatus(ctx, store.TenantDestroyed); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, store.Audit{ActorType: store.ActorSystem, ActorID: "jobs", Action: "tenant.destroyed",
			TargetType: "tenant", TargetID: tenant.String(), Result: store.ResultSuccess,
			Details: map[string]any{"rows_deleted": res.RowsDeleted, "export_files_deleted": res.FilesGone}})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
