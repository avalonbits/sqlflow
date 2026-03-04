package sqlflow_test

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/avalonbits/sqlflow"
)

//go:embed testdata/migrations
var rawEmbedFS embed.FS

// embedFS returns an fs.FS whose root is the migrations directory, suitable
// for passing directly to sqlflow functions.
func embedFS() fs.FS {
	sub, err := fs.Sub(rawEmbedFS, "testdata/migrations")
	if err != nil {
		panic(err)
	}
	return sub
}

// dirFS returns an os.DirFS rooted at testdata/migrations.
func dirFS() fs.FS {
	return os.DirFS("testdata/migrations")
}

// kvQuerier is a stand-in for sqlc-generated code.
type kvQuerier struct {
	db sqlflow.DBTX
}

func (q *kvQuerier) Set(ctx context.Context, key, val string) error {
	_, err := q.db.ExecContext(ctx, `INSERT INTO kv(key, val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val`, key, val)
	return err
}

func (q *kvQuerier) Get(ctx context.Context, key string) (string, error) {
	var val string
	err := q.db.QueryRowContext(ctx, `SELECT val FROM kv WHERE key=?`, key).Scan(&val)
	return val, err
}

func newQuerier() sqlflow.Querier[kvQuerier] {
	return func(tx sqlflow.DBTX) *kvQuerier {
		return &kvQuerier{db: tx}
	}
}

type migrationCase struct {
	name string
	fsys fs.FS
}

func bothMigrations(t *testing.T) []migrationCase {
	t.Helper()
	return []migrationCase{
		{name: "embed", fsys: embedFS()},
		{name: "dir", fsys: dirFS()},
	}
}

// --- Section 1: Migrations constructors ---

func TestEmbedMigrations(t *testing.T) {
	t.Parallel()
	db := sqlflow.TestDB(embedFS(), newQuerier())
	defer db.Close()
	ctx := context.Background()
	if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := db.Read(ctx, func(q *kvQuerier) error {
		var err error
		got, err = q.Get(ctx, "k")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got != "v" {
		t.Fatalf("got %q want %q", got, "v")
	}
}

func TestDirMigrations(t *testing.T) {
	t.Parallel()
	db := sqlflow.TestDB(dirFS(), newQuerier())
	defer db.Close()
	ctx := context.Background()
	if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := db.Read(ctx, func(q *kvQuerier) error {
		var err error
		got, err = q.Get(ctx, "k")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got != "v" {
		t.Fatalf("got %q want %q", got, "v")
	}
}

func TestDirMigrations_BadPath(t *testing.T) {
	t.Parallel()
	for _, mc := range []migrationCase{
		{name: "embed", fsys: embed.FS{}},
		{name: "dir", fsys: os.DirFS("/nonexistent/path/that/does/not/exist")},
	} {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			_, err := sqlflow.GetDB(filepath.Join(dir, "test.db"), mc.fsys, newQuerier())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// --- Section 2: TestDB ---

func TestTestDB(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "hello", "world") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "hello")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "world" {
				t.Fatalf("got %q want %q", got, "world")
			}
		})
	}
}

func TestTestDB_Panic(t *testing.T) {
	t.Parallel()
	for _, mc := range []migrationCase{
		{name: "embed", fsys: embed.FS{}},
		{name: "dir", fsys: os.DirFS("/nonexistent/path")},
	} {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic, got none")
				}
			}()
			sqlflow.TestDB(mc.fsys, newQuerier())
		})
	}
}

// --- Section 3: GetDB ---

func TestGetDB(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db, err := sqlflow.GetDB(filepath.Join(t.TempDir(), "test.db"), mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "a", "1") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "a")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "1" {
				t.Fatalf("got %q want %q", got, "1")
			}
		})
	}
}

func TestGetDB_CreatesDir(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "sub", "nested", "test.db")
			db, err := sqlflow.GetDB(path, mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			if _, err := os.Stat(filepath.Dir(path)); err != nil {
				t.Fatalf("directory not created: %v", err)
			}
		})
	}
}

func TestGetDB_RunsMigrations(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "test.db")
			db, err := sqlflow.GetDB(path, mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			raw, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			var name string
			err = raw.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='kv'`).Scan(&name)
			if err != nil {
				t.Fatalf("table kv not found: %v", err)
			}
		})
	}
}

func TestGetDB_Idempotent(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "test.db")
			db, err := sqlflow.GetDB(path, mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			db2, err := sqlflow.GetDB(path, mc.fsys, newQuerier())
			if err != nil {
				t.Fatalf("second GetDB failed: %v", err)
			}
			db2.Close()
		})
	}
}

func TestGetDB_BadPath(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			// Create a file where a directory is expected.
			base := t.TempDir()
			blocker := filepath.Join(base, "blocker")
			if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := sqlflow.GetDB(filepath.Join(blocker, "test.db"), mc.fsys, newQuerier())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// --- Section 4: GetEncryptedDB ---

func TestGetEncryptedDB(t *testing.T) {
	t.Parallel()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "enc.db")
			db, err := sqlflow.GetEncryptedDB(path, mc.fsys, newQuerier(), key)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "secret", "value") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "secret")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "value" {
				t.Fatalf("got %q want %q", got, "value")
			}
		})
	}
}

func TestGetEncryptedDB_WrongKey(t *testing.T) {
	t.Parallel()
	goodKey := make([]byte, 32)
	for i := range goodKey {
		goodKey[i] = byte(i + 1)
	}
	badKey := make([]byte, 32)
	for i := range badKey {
		badKey[i] = 0xFF
	}
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "enc.db")
			db, err := sqlflow.GetEncryptedDB(path, mc.fsys, newQuerier(), goodKey)
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			db2, err := sqlflow.OpenEncryptedDB(path, mc.fsys, newQuerier(), badKey)
			if err != nil {
				// Some implementations fail at open time.
				return
			}
			defer db2.Close()
			ctx := context.Background()
			err = db2.Read(ctx, func(q *kvQuerier) error {
				_, err := q.Get(ctx, "k")
				return err
			})
			if err == nil {
				t.Fatal("expected error with wrong key, got nil")
			}
		})
	}
}

func TestGetEncryptedDB_CreatesDir(t *testing.T) {
	t.Parallel()
	key := make([]byte, 32)
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "sub", "enc.db")
			db, err := sqlflow.GetEncryptedDB(path, mc.fsys, newQuerier(), key)
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			if _, err := os.Stat(filepath.Dir(path)); err != nil {
				t.Fatalf("directory not created: %v", err)
			}
		})
	}
}

// --- Section 5: OpenDB ---

func TestOpenDB_NewFile(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "new.db")
			db, err := sqlflow.OpenDB(path, mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "x", "y") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "x")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "y" {
				t.Fatalf("got %q want %q", got, "y")
			}
		})
	}
}

func TestOpenDB_ExistingFile(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "existing.db")
			db, err := sqlflow.GetDB(path, mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			db2, err := sqlflow.OpenDB(path, mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			defer db2.Close()
			ctx := context.Background()
			if err := db2.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "p", "q") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db2.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "p")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "q" {
				t.Fatalf("got %q want %q", got, "q")
			}
		})
	}
}

func TestOpenDB_SkipsMigration(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "skip.db")
			db, err := sqlflow.GetDB(path, mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			// OpenDB on existing file must succeed even with a bad migrations value.
			badM := os.DirFS("/nonexistent")
			db2, err := sqlflow.OpenDB(path, badM, newQuerier())
			if err != nil {
				t.Fatalf("OpenDB should skip migration for existing file: %v", err)
			}
			db2.Close()
		})
	}
}

// --- Section 6: OpenEncryptedDB ---

func TestOpenEncryptedDB_NewFile(t *testing.T) {
	t.Parallel()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "new_enc.db")
			db, err := sqlflow.OpenEncryptedDB(path, mc.fsys, newQuerier(), key)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "n", "m") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "n")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "m" {
				t.Fatalf("got %q want %q", got, "m")
			}
		})
	}
}

func TestOpenEncryptedDB_ExistingFile(t *testing.T) {
	t.Parallel()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "enc_exist.db")
			db, err := sqlflow.GetEncryptedDB(path, mc.fsys, newQuerier(), key)
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			db2, err := sqlflow.OpenEncryptedDB(path, mc.fsys, newQuerier(), key)
			if err != nil {
				t.Fatal(err)
			}
			defer db2.Close()
			ctx := context.Background()
			if err := db2.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "e", "f") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db2.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "e")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "f" {
				t.Fatalf("got %q want %q", got, "f")
			}
		})
	}
}

func TestOpenEncryptedDB_WrongKey(t *testing.T) {
	t.Parallel()
	goodKey := make([]byte, 32)
	for i := range goodKey {
		goodKey[i] = byte(i + 1)
	}
	badKey := make([]byte, 32)
	for i := range badKey {
		badKey[i] = 0xAB
	}
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "enc_bad.db")
			db, err := sqlflow.GetEncryptedDB(path, mc.fsys, newQuerier(), goodKey)
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			db2, err := sqlflow.OpenEncryptedDB(path, mc.fsys, newQuerier(), badKey)
			if err != nil {
				return
			}
			defer db2.Close()
			ctx := context.Background()
			err = db2.Read(ctx, func(q *kvQuerier) error {
				_, err := q.Get(ctx, "k")
				return err
			})
			if err == nil {
				t.Fatal("expected error with wrong key, got nil")
			}
		})
	}
}

// --- Section 7: DB.Read / DB.Write ---

func TestDB_WriteRead(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			pairs := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}}
			for _, p := range pairs {
				if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, p[0], p[1]) }); err != nil {
					t.Fatal(err)
				}
			}
			for _, p := range pairs {
				var got string
				if err := db.Read(ctx, func(q *kvQuerier) error {
					var err error
					got, err = q.Get(ctx, p[0])
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if got != p[1] {
					t.Fatalf("key %q: got %q want %q", p[0], got, p[1])
				}
			}
		})
	}
}

func TestDB_WriteOverwrite(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "k", "first") }); err != nil {
				t.Fatal(err)
			}
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "k", "second") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "k")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "second" {
				t.Fatalf("got %q want %q", got, "second")
			}
		})
	}
}

func TestDB_Read_NotFound(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			err := db.Read(ctx, func(q *kvQuerier) error {
				_, err := q.Get(ctx, "missing")
				return err
			})
			if !sqlflow.NoRows(err) {
				t.Fatalf("expected NoRows, got %v", err)
			}
		})
	}
}

func TestDB_ConcurrentReads(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, 50)
			for range 50 {
				wg.Go(func() {
					errs <- db.Read(ctx, func(q *kvQuerier) error {
						_, err := q.Get(ctx, "k")
						return err
					})
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Errorf("concurrent read error: %v", err)
				}
			}
		})
	}
}

func TestDB_ConcurrentWrites(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			errs := make(chan error, 50)
			for i := range 50 {
				wg.Go(func() {
					key := fmt.Sprintf("key%d", i)
					errs <- db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, key, "v") })
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Errorf("concurrent write error: %v", err)
				}
			}
			for i := range 50 {
				var got string
				if err := db.Read(ctx, func(q *kvQuerier) error {
					var err error
					got, err = q.Get(ctx, fmt.Sprintf("key%d", i))
					return err
				}); err != nil {
					t.Errorf("key%d: %v", i, err)
				} else if got != "v" {
					t.Errorf("key%d: got %q want %q", i, got, "v")
				}
			}
		})
	}
}

func TestDB_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "shared", "init") }); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, 50)
			for range 25 {
				wg.Go(func() {
					errs <- db.Read(ctx, func(q *kvQuerier) error {
						_, err := q.Get(ctx, "shared")
						return err
					})
				})
			}
			for i := range 25 {
				wg.Go(func() {
					errs <- db.Write(ctx, func(q *kvQuerier) error {
						return q.Set(ctx, fmt.Sprintf("w%d", i), "v")
					})
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Errorf("concurrent rw error: %v", err)
				}
			}
		})
	}
}

func TestDB_Write_ContextCancel(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			ready := make(chan struct{})
			go func() {
				db.Write(ctx, func(q *kvQuerier) error { //nolint
					close(ready)
					time.Sleep(300 * time.Millisecond)
					return nil
				})
			}()
			<-ready
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()
			err := db.Write(cancelCtx, func(q *kvQuerier) error { return nil })
			if err == nil {
				t.Fatal("expected error from cancelled context, got nil")
			}
		})
	}
}

func TestDB_Write_FuncError(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			sentinel := errors.New("function error")
			err := db.Write(ctx, func(q *kvQuerier) error { return sentinel })
			if !errors.Is(err, sentinel) {
				t.Fatalf("got %v want sentinel error", err)
			}
		})
	}
}

func TestDB_Write_Rollback(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			defer db.Close()
			ctx := context.Background()
			db.Write(ctx, func(q *kvQuerier) error { //nolint
				q.Set(ctx, "ghost", "value") //nolint
				return errors.New("abort")
			})
			err := db.Read(ctx, func(q *kvQuerier) error {
				_, err := q.Get(ctx, "ghost")
				return err
			})
			if !sqlflow.NoRows(err) {
				t.Fatalf("expected NoRows after rollback, got %v", err)
			}
		})
	}
}

// --- Section 8: DB.Checkpoint ---

func TestDB_Checkpoint(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db, err := sqlflow.GetDB(filepath.Join(t.TempDir(), "ckpt.db"), mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			for i := range 5 {
				db.Write(ctx, func(q *kvQuerier) error { //nolint
					return q.Set(ctx, fmt.Sprintf("k%d", i), "v")
				})
			}
			if err := db.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDB_Checkpoint_CancelledCtx(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db, err := sqlflow.GetDB(filepath.Join(t.TempDir(), "ckpt2.db"), mc.fsys, newQuerier())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := db.Checkpoint(ctx); err == nil {
				t.Fatal("expected error with cancelled context")
			}
		})
	}
}

// --- Section 9: DB.Close ---

func TestDB_Close(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			err := db.Read(ctx, func(q *kvQuerier) error {
				_, err := q.Get(ctx, "k")
				return err
			})
			if err == nil {
				t.Fatal("expected error after Close, got nil")
			}
		})
	}
}

func TestDB_Close_Idempotent(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			db := sqlflow.TestDB(mc.fsys, newQuerier())
			db.Close() //nolint
			// Second close should not panic.
			_ = db.Close()
		})
	}
}

// --- Section 10: NoRows ---

func TestNoRows(t *testing.T) {
	t.Parallel()
	if !sqlflow.NoRows(sql.ErrNoRows) {
		t.Error("NoRows(sql.ErrNoRows) = false, want true")
	}
	if sqlflow.NoRows(nil) {
		t.Error("NoRows(nil) = true, want false")
	}
	if sqlflow.NoRows(errors.New("other")) {
		t.Error("NoRows(other error) = true, want false")
	}
	if !sqlflow.NoRows(fmt.Errorf("wrapped: %w", sql.ErrNoRows)) {
		t.Error("NoRows(wrapped ErrNoRows) = false, want true")
	}
}

// --- Section 11: NewPool / TestPool ---

func TestTestPool(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			if err := p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := p.Read(ctx, "alice", func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "k")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got != "v" {
				t.Fatalf("got %q want %q", got, "v")
			}
		})
	}
}

func TestNewPool_CreatesDir(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "sub", "pool")
			p, err := sqlflow.NewPool(dir, mc.fsys, newQuerier(), 1000, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			p.Close()
			if _, err := os.Stat(dir); err != nil {
				t.Fatalf("dir not created: %v", err)
			}
		})
	}
}

func TestNewPool_MigratesExistingDBs(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			// Create a bare, un-migrated SQLite file.
			bare := filepath.Join(dir, "preexist.db")
			rawDB, err := sql.Open("sqlite3", bare)
			if err != nil {
				t.Fatal(err)
			}
			if err := rawDB.Ping(); err != nil {
				t.Fatal(err)
			}
			rawDB.Close()

			p, err := sqlflow.NewPool(dir, mc.fsys, newQuerier(), 1000, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			ctx := context.Background()
			if err := p.Write(ctx, "preexist", func(q *kvQuerier) error { return q.Set(ctx, "m", "n") }); err != nil {
				t.Fatalf("write after MigrateAll: %v", err)
			}
		})
	}
}

func TestNewPool_BadDir(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			base := t.TempDir()
			blocker := filepath.Join(base, "blocker")
			if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := sqlflow.NewPool(filepath.Join(blocker, "pool"), mc.fsys, newQuerier(), 1000, nil, 0)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestNewPool_BadMigration(t *testing.T) {
	t.Parallel()
	for _, mc := range []migrationCase{
		{name: "embed", fsys: embed.FS{}},
		{name: "dir", fsys: os.DirFS("/nonexistent")},
	} {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			// Pre-create a .db file so MigrateAll has something to migrate.
			rawDB, err := sql.Open("sqlite3", filepath.Join(dir, "existing.db"))
			if err != nil {
				t.Fatal(err)
			}
			if err := rawDB.Ping(); err != nil {
				t.Fatal(err)
			}
			rawDB.Close()
			_, err = sqlflow.NewPool(dir, mc.fsys, newQuerier(), 1000, nil, 0)
			if err == nil {
				t.Fatal("expected error with bad migration, got nil")
			}
		})
	}
}

// --- Section 12: Pool.Read / Pool.Write ---

func TestPool_WriteRead(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			keys := []string{"alice", "bob", "carol"}
			for _, k := range keys {
				if err := p.Write(ctx, k, func(q *kvQuerier) error { return q.Set(ctx, "x", k) }); err != nil {
					t.Fatal(err)
				}
			}
			for _, k := range keys {
				var got string
				if err := p.Read(ctx, k, func(q *kvQuerier) error {
					var err error
					got, err = q.Get(ctx, "x")
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if got != k {
					t.Fatalf("key %q: got %q want %q", k, got, k)
				}
			}
		})
	}
}

func TestPool_IsolatedKeys(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "who", "A") }) //nolint
			p.Write(ctx, "bob", func(q *kvQuerier) error { return q.Set(ctx, "who", "B") })   //nolint
			var a, b string
			p.Read(ctx, "alice", func(q *kvQuerier) error { a, _ = q.Get(ctx, "who"); return nil }) //nolint
			p.Read(ctx, "bob", func(q *kvQuerier) error { b, _ = q.Get(ctx, "who"); return nil })   //nolint
			if a != "A" {
				t.Errorf("alice: got %q want A", a)
			}
			if b != "B" {
				t.Errorf("bob: got %q want B", b)
			}
		})
	}
}

func TestPool_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			poolKeys := []string{"u1", "u2", "u3", "u4", "u5"}
			var wg sync.WaitGroup
			errs := make(chan error, 60)
			for i := range 30 {
				wg.Go(func() {
					k := poolKeys[i%len(poolKeys)]
					if i%2 == 0 {
						errs <- p.Write(ctx, k, func(q *kvQuerier) error { return q.Set(ctx, "n", fmt.Sprintf("%d", i)) })
					} else {
						errs <- p.Read(ctx, k, func(q *kvQuerier) error { _, err := q.Get(ctx, "n"); return err })
					}
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil && !sqlflow.NoRows(err) {
					t.Errorf("concurrent pool error: %v", err)
				}
			}
		})
	}
}

func TestPool_Write_FuncError(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			sentinel := errors.New("pool write error")
			err := p.Write(ctx, "alice", func(q *kvQuerier) error { return sentinel })
			if !errors.Is(err, sentinel) {
				t.Fatalf("got %v want sentinel", err)
			}
		})
	}
}

func TestPool_Read_NotFound(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			err := p.Read(ctx, "alice", func(q *kvQuerier) error {
				_, err := q.Get(ctx, "missing")
				return err
			})
			if !sqlflow.NoRows(err) {
				t.Fatalf("expected NoRows, got %v", err)
			}
		})
	}
}

func TestPool_KeyNotAvailable(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p, err := sqlflow.NewPool(
				t.TempDir(), mc.fsys, newQuerier(), 1000,
				func(string) ([]byte, bool) { return nil, false },
				0,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			ctx := context.Background()
			err = p.Write(ctx, "alice", func(q *kvQuerier) error { return nil })
			if !errors.Is(err, sqlflow.ErrKeyNotAvailable) {
				t.Fatalf("got %v want ErrKeyNotAvailable", err)
			}
		})
	}
}

// --- Section 13: Pool.Evict / Pool.Wait ---

func TestPool_Evict(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			if err := p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
				t.Fatal(err)
			}
			p.Evict("alice")
			p.Wait()
			if err := p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "k", "v2") }); err != nil {
				t.Fatalf("write after evict: %v", err)
			}
		})
	}
}

func TestPool_Evict_WhileInFlight(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			started := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- p.Write(ctx, "alice", func(q *kvQuerier) error {
					close(started)
					time.Sleep(150 * time.Millisecond)
					return q.Set(ctx, "k", "v")
				})
			}()
			<-started
			p.Evict("alice")
			if err := <-done; err != nil {
				t.Fatalf("in-flight write failed: %v", err)
			}
		})
	}
}

func TestPool_Evict_NonExistent(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			// Should not panic.
			p.Evict("nobody")
		})
	}
}

// --- Section 14: Pool.MigrateAll ---

func TestPool_MigrateAll(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			for _, name := range []string{"a", "b"} {
				rawDB, err := sql.Open("sqlite3", filepath.Join(dir, name+".db"))
				if err != nil {
					t.Fatal(err)
				}
				if err := rawDB.Ping(); err != nil {
					t.Fatal(err)
				}
				rawDB.Close()
			}
			p, err := sqlflow.NewPool(dir, mc.fsys, newQuerier(), 1000, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			ctx := context.Background()
			for _, k := range []string{"a", "b"} {
				if err := p.Write(ctx, k, func(q *kvQuerier) error { return q.Set(ctx, "x", k) }); err != nil {
					t.Fatalf("key %q: %v", k, err)
				}
			}
		})
	}
}

func TestPool_MigrateAll_SkipsForEncrypted(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			key := make([]byte, 32)
			for i := range key {
				key[i] = byte(i + 1)
			}
			p, err := sqlflow.NewPool(
				dir, mc.fsys, newQuerier(), 1000,
				func(string) ([]byte, bool) { return key, true },
				0,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			ctx := context.Background()
			if err := p.Write(ctx, "user1", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
				t.Fatalf("lazy migration write: %v", err)
			}
		})
	}
}

// --- Section 15: Pool.ListKeys ---

func TestPool_ListKeys(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			ctx := context.Background()
			for _, k := range []string{"alice", "bob", "carol"} {
				if err := p.Write(ctx, k, func(q *kvQuerier) error { return q.Set(ctx, "x", k) }); err != nil {
					t.Fatal(err)
				}
			}
			keys, err := p.ListKeys()
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(keys)
			want := []string{"alice", "bob", "carol"}
			if len(keys) != len(want) {
				t.Fatalf("got %v want %v", keys, want)
			}
			for i, k := range want {
				if keys[i] != k {
					t.Errorf("[%d] got %q want %q", i, keys[i], k)
				}
			}
		})
	}
}

func TestPool_ListKeys_Empty(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			defer p.Close()
			keys, err := p.ListKeys()
			if err != nil {
				t.Fatal(err)
			}
			if len(keys) != 0 {
				t.Fatalf("expected empty, got %v", keys)
			}
		})
	}
}

// --- Section 16: Inactivity reaper ---

func TestPool_InactivityReaper(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p, err := sqlflow.NewPool(t.TempDir(), mc.fsys, newQuerier(), 1000, nil, 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			ctx := context.Background()
			if err := p.Write(ctx, "x", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
				t.Fatal(err)
			}
			time.Sleep(400 * time.Millisecond)
			// Entry should have been reaped; write must still succeed (re-created).
			if err := p.Write(ctx, "x", func(q *kvQuerier) error { return q.Set(ctx, "k", "v2") }); err != nil {
				t.Fatalf("write after reap: %v", err)
			}
		})
	}
}

func TestPool_InactivityReaper_ActiveNotEvicted(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p, err := sqlflow.NewPool(t.TempDir(), mc.fsys, newQuerier(), 1000, nil, 200*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			ctx := context.Background()
			// Keep the entry active by writing every 50ms for 400ms.
			for i := range 8 {
				if err := p.Write(ctx, "active", func(q *kvQuerier) error {
					return q.Set(ctx, "n", fmt.Sprintf("%d", i))
				}); err != nil {
					t.Fatalf("keep-alive write %d: %v", i, err)
				}
				time.Sleep(50 * time.Millisecond)
			}
			// Should still be readable without error.
			if err := p.Read(ctx, "active", func(q *kvQuerier) error {
				_, err := q.Get(ctx, "n")
				return err
			}); err != nil {
				t.Fatalf("read after active period: %v", err)
			}
		})
	}
}

// --- Section 17: Pool.Close ---

func TestPool_Close(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			ctx := context.Background()
			p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }) //nolint
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPool_Close_DrainsInFlight(t *testing.T) {
	t.Parallel()
	for _, mc := range bothMigrations(t) {
		t.Run(mc.name, func(t *testing.T) {
			t.Parallel()
			p := sqlflow.TestPool(t.TempDir(), mc.fsys, newQuerier())
			ctx := context.Background()
			started := make(chan struct{})
			writeErr := make(chan error, 1)
			go func() {
				writeErr <- p.Write(ctx, "alice", func(q *kvQuerier) error {
					close(started)
					time.Sleep(200 * time.Millisecond)
					return q.Set(ctx, "k", "v")
				})
			}()
			<-started
			closeErr := p.Close()
			if err := <-writeErr; err != nil {
				t.Errorf("in-flight write: %v", err)
			}
			if closeErr != nil {
				t.Errorf("Close: %v", closeErr)
			}
		})
	}
}
