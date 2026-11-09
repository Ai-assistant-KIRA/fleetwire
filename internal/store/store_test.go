package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store/storetest"
)

// fixture is one tenant with a device, a key, a chunk, a gap and tokens.
type fixture struct {
	tenant, device, key uuid.UUID
}

func newFixture(t *testing.T, db *store.DB) fixture {
	t.Helper()
	f := fixture{tenant: uuid.New(), device: uuid.New(), key: uuid.New()}
	err := db.InTenantTx(t.Context(), f.tenant, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.CreateTenant(ctx, "t-"+f.tenant.String()[:8], 30); err != nil {
			return err
		}
		if err := tx.InsertTenantKey(ctx, f.key, "kek", []byte("wrapped")); err != nil {
			return err
		}
		if _, err := tx.InsertDevice(ctx, f.device, "dev"); err != nil {
			return err
		}
		now := time.Now()
		if err := tx.InsertChunk(ctx, store.Chunk{DeviceID: f.device, SeqLo: 1, SeqHi: 10, ReadingCount: 10,
			MinCollectedAt: now, MaxCollectedAt: now, KeyID: f.key, Nonce: []byte("n"), Ciphertext: []byte("c")}); err != nil {
			return err
		}
		if err := tx.InsertGap(ctx, store.DeviceGap{DeviceID: f.device, FromSeq: 11, ToSeq: 12, Reason: "DISK_BUDGET"}); err != nil {
			return err
		}
		if err := tx.InsertEnrollmentToken(ctx, uuid.New(), []byte(uuid.NewString()), now.Add(time.Hour), "test"); err != nil {
			return err
		}
		if _, err := tx.InsertAPIToken(ctx, uuid.New(), "tok", []byte("h"), []string{"readings:read"}); err != nil {
			return err
		}
		if _, err := tx.EnqueueJob(ctx, uuid.New(), store.JobRetention, map[string]any{}, "", now); err != nil {
			return err
		}
		if _, err := tx.InsertExport(ctx, uuid.New(), nil); err != nil {
			return err
		}
		if err := tx.SetDeviceCert(ctx, store.DeviceCert{Serial: uuid.NewString(), DeviceID: f.device, NotAfter: now.Add(time.Hour)}); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, store.Audit{ActorType: store.ActorSystem, ActorID: "test", Action: "fixture",
			TargetType: "tenant", TargetID: f.tenant.String(), Result: store.ResultSuccess})
	})
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return f
}

var tenantTables = []string{
	"tenants", "tenant_keys", "devices", "device_gaps", "chunks", "device_certs",
	"enrollment_tokens", "api_tokens", "jobs", "exports", "audit_log",
}

func countRows(ctx context.Context, t *testing.T, conn interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, table string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM fleetwire."+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestRuntimeRoleIsUnprivileged(t *testing.T) {
	db := storetest.AppDB(t)
	if err := db.CheckRuntimeRole(t.Context()); err != nil {
		t.Fatalf("app role rejected: %v", err)
	}
	owner, err := store.Connect(t.Context(), storetest.Start(t).OwnerURL, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := owner.CheckRuntimeRole(t.Context()); err == nil {
		t.Fatal("the owner role was accepted as a runtime role")
	}
}

// TestCrossTenantSQL is P7 at the SQL level: inside tenant A's transaction,
// nothing of tenant B can be read, changed, deleted, or created.
func TestCrossTenantSQL(t *testing.T) {
	db := storetest.AppDB(t)
	a, b := newFixture(t, db), newFixture(t, db)
	conn, err := pgx.Connect(t.Context(), storetest.Start(t).AppURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())

	inTenant := func(tenant uuid.UUID, fn func(tx pgx.Tx)) {
		t.Helper()
		tx, err := conn.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		if _, err := tx.Exec(t.Context(), `SELECT set_config('fleetwire.tenant_id', $1, true)`, tenant.String()); err != nil {
			t.Fatal(err)
		}
		fn(tx)
	}

	inTenant(a.tenant, func(tx pgx.Tx) {
		for _, table := range tenantTables {
			col := "tenant_id"
			if table == "tenants" {
				col = "id"
			}
			var other int
			if err := tx.QueryRow(t.Context(), "SELECT count(*) FROM fleetwire."+table+" WHERE "+col+" = $1", b.tenant).Scan(&other); err != nil {
				t.Fatalf("%s: %v", table, err)
			}
			if other != 0 {
				t.Errorf("%s: tenant A sees %d rows of tenant B", table, other)
			}
		}
		for _, q := range []string{
			`UPDATE fleetwire.devices SET name = 'pwned' WHERE id = $1`,
			`UPDATE fleetwire.tenants SET name = 'pwned' WHERE id = (SELECT tenant_id FROM fleetwire.devices WHERE id = $1)`,
			`DELETE FROM fleetwire.chunks WHERE device_id = $1`,
			`DELETE FROM fleetwire.device_gaps WHERE device_id = $1`,
		} {
			tag, err := tx.Exec(t.Context(), q, b.device)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if tag.RowsAffected() != 0 {
				t.Errorf("%s changed %d rows of tenant B", q, tag.RowsAffected())
			}
		}
	})

	// Writing a row that names tenant B is refused by the WITH CHECK clause.
	inTenant(a.tenant, func(tx pgx.Tx) {
		_, err := tx.Exec(t.Context(), `INSERT INTO fleetwire.devices (id, tenant_id) VALUES ($1, $2)`, uuid.New(), b.tenant)
		if !store.IsPermissionDenied(err) {
			t.Errorf("insert into tenant B: %v, want permission denied", err)
		}
	})
	inTenant(a.tenant, func(tx pgx.Tx) {
		_, err := tx.Exec(t.Context(), `UPDATE fleetwire.devices SET tenant_id = $1 WHERE id = $2`, b.tenant, a.device)
		if !store.IsPermissionDenied(err) {
			t.Errorf("moving a device to tenant B: %v, want permission denied", err)
		}
	})

	// Tenant B's data is untouched.
	err = db.InTenantTx(t.Context(), b.tenant, func(ctx context.Context, tx *store.Tx) error {
		d, err := tx.GetDevice(ctx, b.device)
		if err != nil || d.Name != "dev" {
			t.Errorf("tenant B device = %+v, %v", d, err)
		}
		chunks, err := tx.ChunksBySeq(ctx, b.device, 0, 10)
		if err != nil || len(chunks) != 1 {
			t.Errorf("tenant B chunks = %d, %v", len(chunks), err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMissingTenantSeesNothing(t *testing.T) {
	db := storetest.AppDB(t)
	newFixture(t, db)
	err := db.InTx(t.Context(), func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.GetTenant(ctx); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetTenant without tenant = %v, want ErrNotFound", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(t.Context(), storetest.Start(t).AppURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())
	// Set the placeholder in an earlier transaction first: afterwards
	// current_setting returns '' instead of NULL, which must still match
	// nothing. This is the state of every pooled connection after its
	// first tenant transaction.
	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SELECT set_config('fleetwire.tenant_id', $1, true)`, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, table := range tenantTables {
		if n := countRows(t.Context(), t, conn, table); n != 0 {
			t.Errorf("%s: %d rows visible without a tenant", table, n)
		}
	}
}

func TestExclusionConstraintRejectsOverlap(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	err := db.InTenantTx(t.Context(), f.tenant, func(ctx context.Context, tx *store.Tx) error {
		now := time.Now()
		return tx.InsertChunk(ctx, store.Chunk{DeviceID: f.device, SeqLo: 5, SeqHi: 12, ReadingCount: 8,
			MinCollectedAt: now, MaxCollectedAt: now, KeyID: f.key, Nonce: []byte("n"), Ciphertext: []byte("c")})
	})
	if !store.IsExclusionViolation(err) {
		t.Fatalf("overlapping chunk: %v, want exclusion violation", err)
	}
	err = db.InTenantTx(t.Context(), f.tenant, func(ctx context.Context, tx *store.Tx) error {
		return tx.InsertGap(ctx, store.DeviceGap{DeviceID: f.device, FromSeq: 12, ToSeq: 20, Reason: "LOST"})
	})
	if !store.IsExclusionViolation(err) {
		t.Fatalf("overlapping gap: %v, want exclusion violation", err)
	}
}

func TestAuditLogIsAppendOnly(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	err := db.InTenantTx(t.Context(), f.tenant, func(ctx context.Context, tx *store.Tx) error {
		entries, err := tx.ListAudit(ctx, 0, 10)
		if err != nil || len(entries) != 1 {
			t.Fatalf("audit entries = %v, %v", entries, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, url := range map[string]string{"app": storetest.Start(t).AppURL, "owner": storetest.Start(t).OwnerURL} {
		conn, err := pgx.Connect(t.Context(), url)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{
			`UPDATE fleetwire.audit_log SET result = 'denied'`,
			`DELETE FROM fleetwire.audit_log`,
			`TRUNCATE fleetwire.audit_log`,
		} {
			if _, err := conn.Exec(t.Context(), q); !store.IsPermissionDenied(err) {
				t.Errorf("%s as %s: %v, want permission denied", q, name, err)
			}
		}
		_ = conn.Close(t.Context())
	}
}

func TestFleetWideAuditNeedsNoTenant(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	err := db.InTx(t.Context(), func(ctx context.Context, tx *store.Tx) error {
		return tx.InsertAudit(ctx, store.Audit{ActorType: store.ActorOperator, ActorID: "op", Action: "tenant.list",
			TargetType: "fleet", TargetID: "-", Result: store.ResultSuccess})
	})
	if err != nil {
		t.Fatal(err)
	}
	// A tenant cannot write an entry that claims to be fleet-wide or another tenant's.
	conn, err := pgx.Connect(t.Context(), storetest.Start(t).AppURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())
	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(), `SELECT set_config('fleetwire.tenant_id', $1, true)`, f.tenant.String()); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(t.Context(), `INSERT INTO fleetwire.audit_log (tenant_id, actor_type, actor_id, action, target_type, target_id, result)
		VALUES (NULL, 'system', 'x', 'forged', 'x', 'x', 'success')`)
	if !store.IsPermissionDenied(err) {
		t.Fatalf("forged fleet-wide entry: %v, want permission denied", err)
	}
	err = db.InTx(t.Context(), func(ctx context.Context, tx *store.Tx) error {
		entries, err := tx.OperatorAudit(ctx, 0, 1000)
		if err != nil {
			return err
		}
		var sawFleet, sawTenant bool
		for _, e := range entries {
			sawFleet = sawFleet || (e.TenantID == nil && e.Action == "tenant.list")
			sawTenant = sawTenant || (e.TenantID != nil && *e.TenantID == f.tenant)
		}
		if !sawFleet || !sawTenant {
			t.Errorf("operator audit misses entries: fleet %v tenant %v", sawFleet, sawTenant)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnrollmentTokenIsSingleUse(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	hash, expiredHash := []byte("tok-"+uuid.NewString()), []byte("exp-"+uuid.NewString())
	err := db.InTenantTx(t.Context(), f.tenant, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.InsertEnrollmentToken(ctx, uuid.New(), hash, time.Now().Add(15*time.Minute), "test"); err != nil {
			return err
		}
		return tx.InsertEnrollmentToken(ctx, uuid.New(), expiredHash, time.Now().Add(-time.Second), "test")
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 16
	outcomes := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			err := db.InTx(t.Context(), func(ctx context.Context, tx *store.Tx) error {
				tenant, outcome, err := tx.ConsumeEnrollmentToken(ctx, hash, uuid.New())
				if err == nil && tenant != f.tenant {
					t.Errorf("token resolved to tenant %s", tenant)
				}
				outcomes[i] = outcome
				return err
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	ok := 0
	for _, o := range outcomes {
		switch o {
		case store.EnrollOK:
			ok++
		case store.EnrollUsed:
		default:
			t.Errorf("unexpected outcome %q", o)
		}
	}
	if ok != 1 {
		t.Fatalf("%d concurrent enrollments succeeded with one token, want exactly 1", ok)
	}
	for hash, want := range map[string]string{string(expiredHash): store.EnrollExpired, "nope": store.EnrollUnknown} {
		err := db.InTx(t.Context(), func(ctx context.Context, tx *store.Tx) error {
			_, got, err := tx.ConsumeEnrollmentToken(ctx, []byte(hash), uuid.New())
			if got != want {
				t.Errorf("outcome %q, want %q", got, want)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestClaimJobsNeverHandsOutTwice(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	err := db.InTenantTx(t.Context(), f.tenant, func(ctx context.Context, tx *store.Tx) error {
		for i := 0; i < 50; i++ {
			if _, err := tx.EnqueueJob(ctx, uuid.New(), store.JobExport, map[string]any{"i": i}, "", time.Now()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	seen := map[uuid.UUID]string{}
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			worker := "w" + string(rune('0'+w))
			for {
				var jobs []store.Job
				err := db.InTx(t.Context(), func(ctx context.Context, tx *store.Tx) error {
					var err error
					jobs, err = tx.ClaimJobs(ctx, []string{store.JobExport}, 3, time.Minute, worker)
					return err
				})
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					if prev, dup := seen[j.ID]; dup {
						t.Errorf("job %s claimed by %s and %s", j.ID, prev, worker)
					}
					seen[j.ID] = worker
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	exports := 0
	for range seen {
		exports++
	}
	if exports < 50 {
		t.Fatalf("claimed %d jobs, want at least the 50 enqueued", exports)
	}
}

func TestDedupeKey(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	err := db.InTenantTx(t.Context(), f.tenant, func(ctx context.Context, tx *store.Tx) error {
		key := "retention:" + f.tenant.String() + ":2026-11-09T03"
		first, err := tx.EnqueueJob(ctx, uuid.New(), store.JobRetention, nil, key, time.Now())
		if err != nil {
			return err
		}
		second, err := tx.EnqueueJob(ctx, uuid.New(), store.JobRetention, nil, key, time.Now())
		if err != nil {
			return err
		}
		if !first || second {
			t.Errorf("enqueue results %v %v, want true false", first, second)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
