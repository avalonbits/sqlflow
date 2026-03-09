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
	"github.com/avalonbits/sqlflow/migrators"
)

//go:embed testdata/migrations
var rawEmbedFS embed.FS

// embedFS returns an fs.FS whose root is the migrations directory, suitable
// for passing directly to migrators.Goose.
func embedFS() fs.FS {
	sub, err := fs.Sub(rawEmbedFS, "testdata/migrations")
	if err != nil {
		panic(err)
	}

	return sub
}

// kvQuerier is a minimal query accessor used across tests.
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

func newQuerier() sqlflow.Querier[kvQuerier, sqlflow.DBTX] {
	return func(tx sqlflow.DBTX) *kvQuerier {
		return &kvQuerier{db: tx}
	}
}

// gooseOpt returns the Option that runs goose migrations from embedFS.
func gooseOpt() sqlflow.Option {
	return migrators.Goose(embedFS())
}

type dbCase struct {
	name string
	key  []byte // nil for plain, non-nil for encrypted
}

func dbCases() []dbCase {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}

	return []dbCase{
		{name: "plain", key: nil},
		{name: "encrypted", key: key},
	}
}

// openGetDB calls OpenDB or OpenEncryptedDB based on whether key is nil.
func openDB(path string, key []byte, opts ...sqlflow.Option) (*sqlflow.DB[kvQuerier, sqlflow.DBTX], error) {
	if len(key) > 0 {
		return sqlflow.OpenEncryptedDB(path, newQuerier(), key, opts...)
	}

	return sqlflow.OpenDB(path, newQuerier(), opts...)
}

// --- Section 1: Migrations constructors ---

// --- Section 2: TestDB ---

func TestTestDB(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
	defer db.Close()

	ctx := context.Background()
	if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "hello", "world") }); err != nil {
		t.Fatal(err)
	}

	var got string
	err := db.Read(ctx, func(q *kvQuerier) error {
		var err error
		got, err = q.Get(ctx, "hello")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	if got != "world" {
		t.Fatalf("got %q want %q", got, "world")
	}
}

// --- Section 3: OpenDB / OpenEncryptedDB ---

func TestOpenDB(t *testing.T) {
	t.Parallel()

	for _, dc := range dbCases() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			db, err := openDB(filepath.Join(t.TempDir(), "test.db"), dc.key, migrators.Goose(embedFS()))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()
			if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "a", "1") }); err != nil {
				t.Fatal(err)
			}

			var got string
			err = db.Read(ctx, func(q *kvQuerier) error {
				var err error
				got, err = q.Get(ctx, "a")
				return err
			})
			if err != nil {
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

	for _, dc := range dbCases() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "sub", "nested", "test.db")
			db, err := openDB(path, dc.key, migrators.Goose(embedFS()))
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

func TestGetDB_BadPath(t *testing.T) {
	t.Parallel()

	for _, dc := range dbCases() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			// Create a file where a directory is expected.
			base := t.TempDir()
			blocker := filepath.Join(base, "blocker")
			if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}

			_, err := openDB(filepath.Join(blocker, "test.db"), dc.key, migrators.Goose(embedFS()))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// --- Section 4: OpenEncryptedDB ---

func TestOpenEncryptedDB_WrongKey(t *testing.T) {
	t.Parallel()

	goodKey := make([]byte, 32)
	for i := range goodKey {
		goodKey[i] = byte(i + 1)
	}
	badKey := make([]byte, 32)
	for i := range badKey {
		badKey[i] = 0xFF
	}

	path := filepath.Join(t.TempDir(), "enc.db")
	db, err := sqlflow.OpenEncryptedDB(path, newQuerier(), goodKey, migrators.Goose(embedFS()))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := sqlflow.OpenEncryptedDB(path, newQuerier(), badKey)
	if err != nil {
		t.Fatal(err)
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
}

// --- Section 5: DB.Read / DB.Write ---

func TestDB_WriteRead(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
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
		err := db.Read(ctx, func(q *kvQuerier) error {
			var err error
			got, err = q.Get(ctx, p[0])
			return err
		})
		if err != nil {
			t.Fatal(err)
		}

		if got != p[1] {
			t.Fatalf("key %q: got %q want %q", p[0], got, p[1])
		}
	}
}

func TestDB_WriteOverwrite(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
	defer db.Close()

	ctx := context.Background()
	if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "k", "first") }); err != nil {
		t.Fatal(err)
	}
	if err := db.Write(ctx, func(q *kvQuerier) error { return q.Set(ctx, "k", "second") }); err != nil {
		t.Fatal(err)
	}

	var got string
	err := db.Read(ctx, func(q *kvQuerier) error {
		var err error
		got, err = q.Get(ctx, "k")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	if got != "second" {
		t.Fatalf("got %q want %q", got, "second")
	}
}

func TestDB_Read_NotFound(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
	defer db.Close()

	ctx := context.Background()
	err := db.Read(ctx, func(q *kvQuerier) error {
		_, err := q.Get(ctx, "missing")
		return err
	})
	if !sqlflow.NoRows(err) {
		t.Fatalf("expected NoRows, got %v", err)
	}
}

func TestDB_ConcurrentReads(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
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
}

func TestDB_ConcurrentWrites(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
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
		err := db.Read(ctx, func(q *kvQuerier) error {
			var err error
			got, err = q.Get(ctx, fmt.Sprintf("key%d", i))
			return err
		})
		if err != nil {
			t.Errorf("key%d: %v", i, err)
		} else if got != "v" {
			t.Errorf("key%d: got %q want %q", i, got, "v")
		}
	}
}

func TestDB_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
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
}

func TestDB_Write_ContextCancel(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
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
}

func TestDB_Write_FuncError(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
	defer db.Close()

	ctx := context.Background()
	sentinel := errors.New("function error")
	err := db.Write(ctx, func(q *kvQuerier) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v want sentinel error", err)
	}
}

func TestDB_Write_Rollback(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
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
}

// --- Section 8: DB.Checkpoint ---

func TestDB_Checkpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cancelCtx bool
		wantErr   bool
	}{
		{name: "success", cancelCtx: false, wantErr: false},
		{name: "cancelled context", cancelCtx: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, err := sqlflow.OpenDB(
				filepath.Join(t.TempDir(), "ckpt.db"),
				newQuerier(),
				migrators.Goose(embedFS()),
			)
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

			if tt.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			err = db.Checkpoint(ctx)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatal(err)
			}
		})
	}
}

// --- Section 9: DB.Close ---

func TestDB_Close(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
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
}

func TestDB_Close_Idempotent(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newQuerier(), migrators.Goose(embedFS()))
	db.Close() //nolint

	// Second close should not panic.
	_ = db.Close()
}

// --- Section 9a: DB lifecycle options (OnOpen / OnClose) ---

func TestDB_OnOpen_Called(t *testing.T) {
	t.Parallel()

	for _, dc := range dbCases() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "hooks.db")
			var gotPath string
			db, err := openDB(path, dc.key,
				migrators.Goose(embedFS()),
				sqlflow.OnOpen(func(p string, _ *sql.DB) error {
					gotPath = p
					return nil
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			db.Close()

			if gotPath != path {
				t.Errorf("OnOpen got path %q, want %q", gotPath, path)
			}
		})
	}
}

func TestDB_OnOpen_Error(t *testing.T) {
	t.Parallel()

	for _, dc := range dbCases() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			sentinel := errors.New("open hook failed")
			path := filepath.Join(t.TempDir(), "err.db")
			_, err := openDB(path, dc.key,
				sqlflow.OnOpen(func(string, *sql.DB) error { return sentinel }),
			)
			if !errors.Is(err, sentinel) {
				t.Errorf("got %v, want sentinel error", err)
			}
		})
	}
}

func TestTestDB_OnOpen_Called(t *testing.T) {
	t.Parallel()

	var gotPath string
	db := sqlflow.TestDB(newQuerier(),
		sqlflow.OnOpen(func(p string, _ *sql.DB) error {
			gotPath = p
			return nil
		}),
	)
	defer db.Close()

	if gotPath != ":memory:" {
		t.Errorf("TestDB OnOpen got path %q, want %q", gotPath, ":memory:")
	}
}

func TestTestDB_OnOpen_Error_Panics(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from OnOpen error, got none")
		}
	}()

	sqlflow.TestDB(newQuerier(),
		sqlflow.OnOpen(func(string, *sql.DB) error {
			return errors.New("hook failure")
		}),
	)
}

func TestDB_OnClose_Called(t *testing.T) {
	t.Parallel()

	for _, dc := range dbCases() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			var called bool
			db, err := openDB(
				filepath.Join(t.TempDir(), "close.db"),
				dc.key,
				migrators.Goose(embedFS()),
				sqlflow.OnClose(func(_ string, _ *sql.DB) { called = true }),
			)
			if err != nil {
				t.Fatal(err)
			}

			db.Close()

			if !called {
				t.Error("OnClose was not called after Close()")
			}
		})
	}
}

func TestDB_OnClose_CalledOnceOnDoubleClose(t *testing.T) {
	t.Parallel()

	var count int
	db, err := openDB(
		filepath.Join(t.TempDir(), "twice.db"),
		nil,
		migrators.Goose(embedFS()),
		sqlflow.OnClose(func(_ string, _ *sql.DB) { count++ }),
	)
	if err != nil {
		t.Fatal(err)
	}

	db.Close() //nolint
	db.Close() //nolint

	if count != 1 {
		t.Errorf("OnClose called %d times, want 1", count)
	}
}

func TestTestDB_OnClose_Called(t *testing.T) {
	t.Parallel()

	var called bool
	db := sqlflow.TestDB(newQuerier(),
		sqlflow.OnClose(func(_ string, _ *sql.DB) { called = true }),
	)

	db.Close() //nolint

	if !called {
		t.Error("OnClose was not called after TestDB.Close()")
	}
}

func TestDB_OnClose_ReceivesPath(t *testing.T) {
	t.Parallel()

	// Verify that OnClose receives the same path that was passed to OnOpen.
	var openedPath, closedPath string

	db, err := openDB(
		filepath.Join(t.TempDir(), "shared.db"),
		nil,
		migrators.Goose(embedFS()),
		sqlflow.OnOpen(func(p string, _ *sql.DB) error {
			openedPath = p
			return nil
		}),
		sqlflow.OnClose(func(p string, _ *sql.DB) { closedPath = p }),
	)
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	if closedPath == "" || closedPath != openedPath {
		t.Errorf("OnClose path %q does not match OnOpen path %q", closedPath, openedPath)
	}
}

// --- Section 10: NoRows ---

func TestNoRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "sql.ErrNoRows", err: sql.ErrNoRows, want: true},
		{name: "nil", err: nil, want: false},
		{name: "other error", err: errors.New("other"), want: false},
		{name: "wrapped ErrNoRows", err: fmt.Errorf("wrapped: %w", sql.ErrNoRows), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := sqlflow.NoRows(tt.err); got != tt.want {
				t.Errorf("NoRows(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// --- Section 11: NewPool / TestPool ---

func TestTestPool(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(), gooseOpt())
	defer p.Close()

	ctx := context.Background()
	if err := p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
		t.Fatal(err)
	}

	var got string
	err := p.Read(ctx, "alice", func(q *kvQuerier) error {
		var err error
		got, err = q.Get(ctx, "k")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	if got != "v" {
		t.Fatalf("got %q want %q", got, "v")
	}
}

func TestNewPool_CreatesDir(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "sub", "pool")
	p, err := sqlflow.NewPool(dir, newQuerier(), 1000, gooseOpt())
	if err != nil {
		t.Fatal(err)
	}
	p.Close()

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("dir not created: %v", err)
	}
}

func TestNewPool_BadDir(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	blocker := filepath.Join(base, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := sqlflow.NewPool(filepath.Join(blocker, "pool"), newQuerier(), 1000, gooseOpt())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- Section 11a: Pool-level DB options ---

func TestPool_OnOpen_Called(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var openedPaths []string

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
		sqlflow.OnOpen(func(path string, _ *sql.DB) error {
			mu.Lock()
			openedPaths = append(openedPaths, filepath.Base(path))
			mu.Unlock()
			return nil
		}),
	)
	defer p.Close()

	ctx := context.Background()
	for _, k := range []string{"alice", "bob"} {
		if err := p.Write(ctx, k, func(q *kvQuerier) error { return q.Set(ctx, "x", k) }); err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	n := len(openedPaths)
	mu.Unlock()

	if n != 2 {
		t.Errorf("OnOpen called %d times, want 2", n)
	}
}

func TestPool_OnClose_OnEvict(t *testing.T) {
	t.Parallel()

	var closed bool

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
		sqlflow.OnClose(func(_ string, _ *sql.DB) { closed = true }),
	)

	ctx := context.Background()
	if err := p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
		t.Fatal(err)
	}

	p.Evict("alice")
	p.Wait()

	if !closed {
		t.Error("OnClose not called after eviction")
	}

	p.Close()
}

func TestPool_OnClose_OnPoolClose(t *testing.T) {
	t.Parallel()

	var closed bool

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
		sqlflow.OnClose(func(_ string, _ *sql.DB) { closed = true }),
	)

	ctx := context.Background()
	if err := p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
		t.Fatal(err)
	}

	p.Close()

	if !closed {
		t.Error("OnClose not called after Pool.Close")
	}
}

func TestPool_OnOpen_Error(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("db open hook failed")

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		sqlflow.OnOpen(func(string, *sql.DB) error { return sentinel }),
	)
	defer p.Close()

	ctx := context.Background()
	err := p.Write(ctx, "alice", func(q *kvQuerier) error { return nil })
	if !errors.Is(err, sentinel) {
		t.Errorf("got %v, want sentinel error", err)
	}
}

// --- Section 12: Pool.Read / Pool.Write ---

func TestPool_WriteRead(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
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
		err := p.Read(ctx, k, func(q *kvQuerier) error {
			var err error
			got, err = q.Get(ctx, "x")
			return err
		})
		if err != nil {
			t.Fatal(err)
		}

		if got != k {
			t.Fatalf("key %q: got %q want %q", k, got, k)
		}
	}
}

func TestPool_IsolatedKeys(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
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
}

func TestPool_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
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
}

func TestPool_Write_FuncError(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
	defer p.Close()

	ctx := context.Background()
	sentinel := errors.New("pool write error")
	err := p.Write(ctx, "alice", func(q *kvQuerier) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v want sentinel", err)
	}
}

func TestPool_Read_NotFound(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
	defer p.Close()

	ctx := context.Background()
	err := p.Read(ctx, "alice", func(q *kvQuerier) error {
		_, err := q.Get(ctx, "missing")
		return err
	})
	if !sqlflow.NoRows(err) {
		t.Fatalf("expected NoRows, got %v", err)
	}
}

func TestPool_KeyNotAvailable(t *testing.T) {
	t.Parallel()

	p, err := sqlflow.NewEncryptedPool(
		t.TempDir(), newQuerier(), 1000,
		func(string) ([]byte, bool) { return nil, false },
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
}

// --- Section 13: Pool.Evict / Pool.Wait ---

func TestPool_Evict(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
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
}

func TestPool_Evict_WhileInFlight(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
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
}

func TestPool_Evict_NonExistent(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
	defer p.Close()

	// Should not panic.
	p.Evict("nobody")
}

// --- Section 14: Pool.ListKeys ---

func TestPool_ListKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		seedKeys []string
		want     []string
	}{
		{
			name:     "populated",
			seedKeys: []string{"alice", "bob", "carol"},
			want:     []string{"alice", "bob", "carol"},
		},
		{
			name:     "empty",
			seedKeys: nil,
			want:     []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := sqlflow.TestPool(t.TempDir(), newQuerier(),
				gooseOpt(),
			)
			defer p.Close()

			ctx := context.Background()
			for _, k := range tt.seedKeys {
				if err := p.Write(ctx, k, func(q *kvQuerier) error { return q.Set(ctx, "x", k) }); err != nil {
					t.Fatal(err)
				}
			}

			keys, err := p.ListKeys()
			if err != nil {
				t.Fatal(err)
			}

			sort.Strings(keys)
			if len(keys) != len(tt.want) {
				t.Fatalf("got %v want %v", keys, tt.want)
			}
			for i, k := range tt.want {
				if keys[i] != k {
					t.Errorf("[%d] got %q want %q", i, keys[i], k)
				}
			}
		})
	}
}

// --- Section 16: Inactivity reaper ---

func TestPool_InactivityReaper(t *testing.T) {
	t.Parallel()

	p, err := sqlflow.NewPool(t.TempDir(), newQuerier(), 1000, gooseOpt())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.SetInactivityTimeout(100 * time.Millisecond)

	ctx := context.Background()
	if err := p.Write(ctx, "x", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }); err != nil {
		t.Fatal(err)
	}

	time.Sleep(400 * time.Millisecond)

	// Entry should have been reaped; write must still succeed (re-created).
	if err := p.Write(ctx, "x", func(q *kvQuerier) error { return q.Set(ctx, "k", "v2") }); err != nil {
		t.Fatalf("write after reap: %v", err)
	}
}

func TestPool_InactivityReaper_ActiveNotEvicted(t *testing.T) {
	t.Parallel()

	p, err := sqlflow.NewPool(t.TempDir(), newQuerier(), 1000, gooseOpt())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.SetInactivityTimeout(200 * time.Millisecond)

	ctx := context.Background()
	// Keep the entry active by writing every 50ms for 400ms.
	for i := range 8 {
		err := p.Write(ctx, "active", func(q *kvQuerier) error {
			return q.Set(ctx, "n", fmt.Sprintf("%d", i))
		})
		if err != nil {
			t.Fatalf("keep-alive write %d: %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Should still be readable without error.
	err = p.Read(ctx, "active", func(q *kvQuerier) error {
		_, err := q.Get(ctx, "n")
		return err
	})
	if err != nil {
		t.Fatalf("read after active period: %v", err)
	}
}

// --- Section 17: Pool.Close ---

func TestPool_Close(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
	ctx := context.Background()
	p.Write(ctx, "alice", func(q *kvQuerier) error { return q.Set(ctx, "k", "v") }) //nolint

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPool_Close_DrainsInFlight(t *testing.T) {
	t.Parallel()

	p := sqlflow.TestPool(t.TempDir(), newQuerier(),
		gooseOpt(),
	)
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
}
