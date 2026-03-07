package migrators_test

import (
	"context"
	"embed"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/avalonbits/sqlflow"
	"github.com/avalonbits/sqlflow/migrators"
)

//go:embed testdata/migrations
var rawEmbedFS embed.FS

func embedFS() fs.FS {
	sub, err := fs.Sub(rawEmbedFS, "testdata/migrations")
	if err != nil {
		panic(err)
	}

	return sub
}

func dirFS() fs.FS {
	return os.DirFS("testdata/migrations")
}

// testQuerier wraps a DBTX to allow raw SQL execution inside sqlflow transactions.
type testQuerier struct{ db sqlflow.DBTX }

func newTestQuerier() sqlflow.Querier[testQuerier] {
	return func(tx sqlflow.DBTX) *testQuerier { return &testQuerier{db: tx} }
}

func TestGoose_EmbedFS(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newTestQuerier(), migrators.Goose(embedFS()))
	defer db.Close()

	ctx := context.Background()
	if err := db.Write(ctx, func(q *testQuerier) error {
		_, err := q.db.ExecContext(ctx, `INSERT INTO kv(key, val) VALUES(?, ?)`, "k", "v")
		return err
	}); err != nil {
		t.Fatalf("table kv not created by embed.FS migration: %v", err)
	}
}

func TestGoose_DirFS(t *testing.T) {
	t.Parallel()

	db := sqlflow.TestDB(newTestQuerier(), migrators.Goose(dirFS()))
	defer db.Close()

	ctx := context.Background()
	if err := db.Write(ctx, func(q *testQuerier) error {
		_, err := q.db.ExecContext(ctx, `INSERT INTO kv(key, val) VALUES(?, ?)`, "k", "v")
		return err
	}); err != nil {
		t.Fatalf("table kv not created by os.DirFS migration: %v", err)
	}
}

func TestGoose_Idempotent(t *testing.T) {
	t.Parallel()

	// Opening the same DB file twice with Goose must succeed (no-op second run).
	path := filepath.Join(t.TempDir(), "test.db")
	gooseOpt := migrators.Goose(embedFS())

	db1, err := sqlflow.GetDB(path, newTestQuerier(), gooseOpt)
	if err != nil {
		t.Fatal(err)
	}
	db1.Close()

	db2, err := sqlflow.GetDB(path, newTestQuerier(), gooseOpt)
	if err != nil {
		t.Fatalf("second open after migration: %v", err)
	}
	db2.Close()
}

func TestGoose_BadFS(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		fsys fs.FS
	}{
		{name: "empty embed", fsys: embed.FS{}},
		{name: "nonexistent dir", fsys: os.DirFS("/nonexistent/path")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := sqlflow.GetDB(
				filepath.Join(t.TempDir(), "test.db"),
				newTestQuerier(),
				migrators.Goose(tc.fsys),
			)
			if err == nil {
				t.Fatal("expected error with bad FS, got nil")
			}
		})
	}
}
