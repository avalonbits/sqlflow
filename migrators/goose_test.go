package migrators_test

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"
	"os"
	"testing"

	"github.com/avalonbits/sqlflow/migrators"
	"github.com/avalonbits/sqlflow/options"

	_ "github.com/mattn/go-sqlite3"
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

// applyGoose opens an in-memory SQLite database, runs Goose migrations from
// fsys via the OnOpen hook, and returns the open connection.
func applyGoose(t *testing.T, fsys fs.FS) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	opt := migrators.Goose[any](fsys)
	var cfg options.Config[any]
	opt(&cfg)

	if err := cfg.OnOpenFn("", db); err != nil {
		db.Close()
		t.Fatalf("Goose OnOpen: %v", err)
	}

	return db
}

func TestGoose_EmbedFS(t *testing.T) {
	t.Parallel()

	db := applyGoose(t, embedFS())
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO kv(key, val) VALUES(?, ?)`, "k", "v"); err != nil {
		t.Fatalf("table kv not created by embed.FS migration: %v", err)
	}
}

func TestGoose_DirFS(t *testing.T) {
	t.Parallel()

	db := applyGoose(t, dirFS())
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO kv(key, val) VALUES(?, ?)`, "k", "v"); err != nil {
		t.Fatalf("table kv not created by os.DirFS migration: %v", err)
	}
}

func TestGoose_Idempotent(t *testing.T) {
	t.Parallel()

	// Running Goose twice on the same connection must succeed (no-op second run).
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fsys := embedFS()
	opt := migrators.Goose[any](fsys)
	var cfg options.Config[any]
	opt(&cfg)

	for i := range 2 {
		if err := cfg.OnOpenFn("", db); err != nil {
			t.Fatalf("Goose run %d: %v", i+1, err)
		}
	}
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

			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			opt := migrators.Goose[any](tc.fsys)
			var cfg options.Config[any]
			opt(&cfg)

			if err := cfg.OnOpenFn("", db); err == nil {
				t.Fatal("expected error with bad FS, got nil")
			}
		})
	}
}
