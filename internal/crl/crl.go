// Package crl publishes the device certificate revocation list.
//
// The API builds the CRL from the database after every revocation and once
// a day, signs it with the Device Issuing CA, and writes it atomically to a
// file the broker reads. The broker picks it up on SIGHUP (see
// docs/reference/third-party-facts.md), which a watcher next to Mosquitto
// sends when the file changes.
package crl

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

// Validity is the CRL's nextUpdate distance. OpenSSL rejects every client
// once it passes, so the publisher refreshes far more often than this.
const Validity = 7 * 24 * time.Hour

// Publisher writes the CRL file.
type Publisher struct {
	db      *store.DB
	ca      *pki.CA
	path    string
	log     *slog.Logger
	trigger chan struct{}
	mu      sync.Mutex
	lastOK  atomic.Int64
	// latest is the last CRL written, served by GET /v1/crl for brokers that
	// do not share the API's file system (Kubernetes).
	latest atomic.Pointer[[]byte]
}

// NewPublisher returns a publisher writing to path.
func NewPublisher(db *store.DB, ca *pki.CA, path string, log *slog.Logger) *Publisher {
	return &Publisher{db: db, ca: ca, path: path, log: log, trigger: make(chan struct{}, 1)}
}

// Publish rebuilds and writes the CRL now.
func (p *Publisher) Publish(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var revoked []store.RevokedCert
	err := p.db.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		var err error
		revoked, err = tx.RevokedCerts(ctx)
		return err
	})
	if err != nil {
		return fmt.Errorf("crl: list revoked certificates: %w", err)
	}
	entries := make([]pki.RevokedEntry, len(revoked))
	for i, r := range revoked {
		entries[i] = pki.RevokedEntry{Serial: r.Serial, RevokedAt: r.RevokedAt}
	}
	pemBytes, err := p.ca.BuildCRL(entries, time.Now(), Validity)
	if err != nil {
		return err
	}
	if err := writeAtomic(p.path, pemBytes); err != nil {
		return err
	}
	p.latest.Store(&pemBytes)
	p.lastOK.Store(time.Now().UnixNano())
	p.log.Info("crl published", "path", p.path, "revoked", len(entries))
	return nil
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".crl-*.tmp")
	if err != nil {
		return fmt.Errorf("crl: temp file: %w", err)
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("crl: write: %w", err)
	}
	if err := f.Chmod(0o644); err != nil {
		_ = f.Close()
		return fmt.Errorf("crl: chmod: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("crl: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("crl: close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("crl: rename: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("crl: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("crl: fsync dir: %w", err)
	}
	return nil
}

// Latest returns the last CRL this process published (PEM), or nil before
// the first publish.
func (p *Publisher) Latest() []byte {
	if b := p.latest.Load(); b != nil {
		return *b
	}
	return nil
}

// Trigger asks Run to publish soon. It never blocks.
func (p *Publisher) Trigger() {
	select {
	case p.trigger <- struct{}{}:
	default:
	}
}

// Age is the time since the last successful publish by this process, or
// a very large value if it never succeeded.
func (p *Publisher) Age() time.Duration {
	last := p.lastOK.Load()
	if last == 0 {
		return 1<<63 - 1
	}
	return time.Since(time.Unix(0, last))
}

// finalPublishLimit bounds the publish Run still makes after ctx ended.
const finalPublishLimit = 5 * time.Second

// Run publishes at start, on Trigger, and every interval, until ctx ends.
// Failures are retried every minute. A Trigger not acted on when ctx ends,
// or whose publish ctx cut off, is still published, within
// finalPublishLimit: the API stops Run once its listeners have drained,
// and the last request may have revoked a device.
func (p *Publisher) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.publishPending(context.WithoutCancel(ctx))
			return
		case <-t.C:
		case <-p.trigger:
		}
		next := interval
		if err := p.Publish(ctx); err != nil {
			if ctx.Err() != nil {
				// Stopped mid-publish: publish once more on the way out.
				p.Trigger()
				p.publishPending(context.WithoutCancel(ctx))
				return
			}
			p.log.Error("crl publish failed", "err", err)
			next = time.Minute
		}
		t.Reset(next)
	}
}

// publishPending publishes if a Trigger is waiting.
func (p *Publisher) publishPending(ctx context.Context) {
	select {
	case <-p.trigger:
	default:
		return
	}
	ctx, cancel := context.WithTimeout(ctx, finalPublishLimit)
	defer cancel()
	if err := p.Publish(ctx); err != nil {
		p.log.Error("crl publish at shutdown failed; the next start publishes it", "err", err)
	}
}
