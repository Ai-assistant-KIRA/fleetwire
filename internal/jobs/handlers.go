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
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

type retentionResult struct {
	Cutoff           time.Time `json:"cutoff"`
	ChunksDeleted    int64     `json:"chunks_deleted"`
	GapsDeleted      int64     `json:"gaps_deleted"`
	TempFilesDeleted int       `json:"temp_files_deleted"`
	ExportsExpired   int       `json:"exports_expired"`
	ExportsDeleted   int       `json:"exports_deleted"`
}

// exportKeep is how long an export stays after it expired or failed, so
// that GET /v1/exports/{id} still says what became of it. Retention then
// deletes it, as finished jobs are deleted 30 days after they finish
// (migration 00008). A failed export has no expiry, so its 30 days count
// from its creation.
const exportKeep = 30 * 24 * time.Hour

// tempSuffix ends the name of every export file still being written.
const tempSuffix = ".tmp"

// staleTempAge is how long an export temp file goes unchanged before
// retention deletes it. A running export writes to its file all the time,
// so one untouched for this long belongs to an attempt that died.
const staleTempAge = time.Hour

// removeStaleTemps deletes the tenant's export temp files that no attempt
// has written to for staleTempAge. They hold decrypted readings.
func (w *Worker) removeStaleTemps(tenant uuid.UUID) (int, error) {
	dir := filepath.Join(w.cfg.ExportDir, tenant.String())
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("list export dir: %w", err)
	}
	n := 0
	for _, e := range entries {
		if !e.Type().IsRegular() || !strings.HasSuffix(e.Name(), tempSuffix) {
			continue
		}
		info, err := e.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return n, fmt.Errorf("stat export temp file: %w", err)
		}
		if w.now().Sub(info.ModTime()) < staleTempAge {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return n, fmt.Errorf("remove stale export temp file: %w", err)
		}
		n++
	}
	return n, nil
}

// retention deletes readings and gap records older than the tenant's
// retention period, export files past their expiry, and exports exportKeep
// after they expired or failed.
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
	if res.TempFilesDeleted, err = w.removeStaleTemps(tenant); err != nil {
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
			break
		}
		for _, e := range expired {
			if e.FilePath != nil {
				if _, err := w.removeExportFile(*e.FilePath); err != nil {
					return nil, err
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
	if res.ExportsDeleted, err = w.deleteOldExports(ctx, tenant); err != nil {
		return nil, err
	}
	return res, nil
}

// deleteOldExports deletes the tenant's exports exportKeep after they
// expired or failed. An expired export normally has no file left; a path
// still recorded is deleted first, so deleting the row never leaves a file
// that nothing names.
func (w *Worker) deleteOldExports(ctx context.Context, tenant uuid.UUID) (int, error) {
	cutoff := w.now().Add(-exportKeep)
	n := 0
	for {
		var old []store.Export
		err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
			var err error
			old, err = tx.OldExports(ctx, cutoff, 100)
			return err
		})
		if err != nil {
			return n, err
		}
		if len(old) == 0 {
			return n, nil
		}
		for _, e := range old {
			if e.FilePath != nil {
				if _, err := w.removeExportFile(*e.FilePath); err != nil {
					return n, err
				}
			}
			if err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
				return tx.DeleteExport(ctx, e.ID)
			}); err != nil {
				return n, err
			}
			n++
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
	// Skipped says why no file was produced, when none was.
	Skipped string `json:"skipped,omitempty"`
}

// Reasons an export job produces no file.
const (
	skipExportGone   = "the export no longer exists"
	skipTenantGone   = "the tenant is being destroyed"
	skipExportFinish = "another attempt finished the export"
)

// export writes the tenant's (or one device's) readings as gzip NDJSON,
// hashing the file while writing it. A retry starts over from a fresh temp
// file, so a crash never leaves a half-written export marked ready.
//
// An export does not start once its tenant is no longer active, and one
// that started before gives up when it finishes (finishExport). A tenant
// destroy waits for those before it removes the tenant's export directory.
//
// The tenant's export directory is made only in the transaction that holds
// the tenant row FOR SHARE and finds the tenant active. A destroy marks the
// tenant destroying before it removes the directory, and that waits for
// the lock, so an attempt that stalled past its lease, while another
// worker gave the export up and the destroy finished, cannot make the
// directory again: its temp file then has no directory to go in.
func (w *Worker) export(ctx context.Context, tenant, id uuid.UUID) (any, error) {
	var exp store.Export
	var skip string
	dir := filepath.Join(w.cfg.ExportDir, tenant.String())
	made := false
	err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		t, err := tx.ShareLockTenant(ctx)
		if err != nil {
			return err
		}
		exp, err = tx.GetExport(ctx, id)
		switch {
		case errors.Is(err, store.ErrNotFound):
			// Deleted by a tenant destroy.
			skip = skipExportGone
			return nil
		case err != nil:
			return err
		case t.Status != store.TenantActive:
			skip = skipTenantGone
			return tx.FailExport(ctx, id, skipTenantGone)
		case exp.Status == store.ExportPending:
			if err := tx.SetExportRunning(ctx, id); err != nil {
				return err
			}
		case exp.Status != store.ExportRunning:
			return nil
		}
		made, err = makeDir(dir)
		return err
	})
	if err != nil {
		if made {
			// The lock may have ended with the session before the directory
			// was made, so a destroy may have run since. An empty directory
			// is made again by the next attempt.
			_ = os.Remove(dir)
		}
		return nil, err
	}
	if skip != "" {
		return exportResult{Skipped: skip}, nil
	}
	if exp.Status != store.ExportPending && exp.Status != store.ExportRunning {
		// Ready, expired or failed: there is nothing left to do.
		return exportResult{}, nil
	}
	if w.exportStarted != nil {
		w.exportStarted()
	}

	final := filepath.Join(dir, id.String()+".ndjson.gz")
	// Each attempt writes its own temp file. Two attempts of one export can
	// overlap, when a worker stalls past its lease and another one takes the
	// job over, and with one shared file each truncated and wrote into the
	// other's stream. Temp files of attempts that died are removed by
	// retention (removeStaleTemps).
	f, err := os.CreateTemp(dir, id.String()+".ndjson.gz.*"+tempSuffix)
	if err != nil {
		return nil, fmt.Errorf("create export file: %w", err)
	}
	tmp := f.Name()
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
	sum := hash.Sum(nil)
	res := exportResult{Readings: readings, Bytes: counter.n, SHA256: fmt.Sprintf("%x", sum)}
	if res.Skipped, err = w.finishExport(ctx, tenant, id, tmp, final, sum, res); err != nil {
		return nil, err
	}
	if res.Skipped != "" {
		return exportResult{Skipped: res.Skipped}, nil
	}
	return res, nil
}

// finishExport moves an export's temp file into place and marks the export
// ready, in one transaction that holds the tenant row FOR SHARE and the
// export row FOR UPDATE. A tenant destroy that committed first is seen,
// and the file is not kept; one that starts later waits, then finds the
// file through the export row and deletes it. Of two overlapping attempts,
// the second waits and then sees the export ready, so it never replaces
// the first one's file. finishExport returns why it gave up, or "".
func (w *Worker) finishExport(ctx context.Context, tenant, id uuid.UUID, tmp, final string, sum []byte, res exportResult) (string, error) {
	var skip string
	renamed, recorded := false, false
	err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		t, err := tx.ShareLockTenant(ctx)
		if err != nil {
			return err
		}
		if t.Status != store.TenantActive {
			skip = skipTenantGone
			return nil
		}
		exp, err := tx.LockExport(ctx, id)
		if errors.Is(err, store.ErrNotFound) {
			skip = skipExportGone
			return nil
		}
		if err != nil {
			return err
		}
		if exp.Status != store.ExportRunning {
			skip = skipExportFinish
			return nil
		}
		if err := os.Rename(tmp, final); err != nil {
			return fmt.Errorf("rename export: %w", err)
		}
		renamed = true
		if err := syncDir(filepath.Dir(final)); err != nil {
			return err
		}
		if err := tx.CompleteExport(ctx, id, final, sum, res.Bytes, res.Readings, w.now().Add(w.cfg.ExportTTL)); err != nil {
			return err
		}
		recorded = true
		return nil
	})
	if err != nil && renamed && !recorded {
		// Rolled back before the commit, so no row names the file. When only
		// the commit failed its outcome is unknown, and the file stays: if
		// the export is still running, the next attempt replaces it.
		_ = os.Remove(final)
	}
	return skip, err
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

// removeExportFile deletes an export file whose path comes from the
// database, and reports whether the path was one it may delete. Only paths
// inside ExportDir are deleted, through os.Root, which also refuses
// symlinks that lead out of it. A path outside is logged and left alone
// rather than failing the job: retention and destroy must not get stuck on
// a row someone edited or on an ExportDir that moved.
func (w *Worker) removeExportFile(path string) (bool, error) {
	rel, err := filepath.Rel(w.cfg.ExportDir, path)
	if err != nil || !filepath.IsLocal(rel) {
		w.d.Log.Error("export file is outside the export directory; not deleting it",
			"path", path, "export_dir", w.cfg.ExportDir)
		return false, nil
	}
	root, err := os.OpenRoot(w.cfg.ExportDir)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("open export dir: %w", err)
	}
	defer root.Close()
	if err := root.Remove(rel); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("remove export file: %w", err)
	}
	return true, nil
}

// makeDir makes dir and any missing parent, and reports whether dir itself
// was made by this call.
func makeDir(dir string) (bool, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return false, fmt.Errorf("export dir: %w", err)
	}
	err := os.Mkdir(dir, 0o700)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("export dir: %w", err)
	}
	return true, nil
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

// errExportRunning fails a destroy attempt that has to wait for an export.
var errExportRunning = errors.New("an export of the tenant is still running; the destroy retries")

// removeTenantExportDir deletes the tenant's export directory with whatever
// is left in it: temp files of attempts that died, and files whose rows
// were lost.
func (w *Worker) removeTenantExportDir(tenant uuid.UUID) error {
	root, err := os.OpenRoot(w.cfg.ExportDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open export dir: %w", err)
	}
	defer root.Close()
	if err := root.RemoveAll(tenant.String()); err != nil {
		return fmt.Errorf("remove the tenant's export dir: %w", err)
	}
	return nil
}

type destroyResult struct {
	RowsDeleted int64 `json:"rows_deleted"`
	FilesGone   int   `json:"export_files_deleted"`
}

// destroyTenant finishes a tenant destroy. Each step checks
// the current state, so a crashed attempt continues where it stopped:
//  1. tenant destroying, devices revoked, wrapped keys gone (normally done
//     by the API call already);
//  2. the tenant's KEK destroyed: from here on no copy of its data,
//     backups included, can be decrypted;
//  3. rows and export files deleted in batches;
//  4. once no export of the tenant is running, its export directory
//     removed;
//  5. tenant marked destroyed; the tenant row and audit log stay.
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
		ok, err := w.removeExportFile(f)
		if err != nil {
			return nil, err
		}
		if ok {
			res.FilesGone++
		}
	}
	if res.RowsDeleted, err = w.deleteInBatches(ctx, tenant, func(ctx context.Context, tx *store.Tx) (int64, error) {
		return tx.DeleteTenantData(ctx, w.cfg.BatchSize)
	}); err != nil {
		return nil, err
	}
	// An export that started while the tenant was active may still be
	// writing a temp file into the tenant's export directory, and gives up
	// when it finishes. The directory is removed once none is running;
	// until then this attempt fails and the job retries.
	var running bool
	if err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		running, err = tx.HasRunningJob(ctx, store.JobExport)
		return err
	}); err != nil {
		return nil, err
	}
	if running {
		return nil, errExportRunning
	}
	if err := w.removeTenantExportDir(tenant); err != nil {
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
