//go:build !modernc && !ncruces

package sqlflow_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing/fstest"

	"github.com/avalonbits/sqlflow"
	"github.com/avalonbits/sqlflow/drivers/mattn"
	"github.com/avalonbits/sqlflow/migrators"
)

// migrations is an in-memory goose migration set. In production use
// //go:embed with fs.Sub, or os.DirFS, to point at real .sql files.
var exampleMigrations = fstest.MapFS{
	"001_init.sql": {
		Data: []byte(`
-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);

-- +goose Down
DROP TABLE kv;
		`),
	},
}

// kvStore wraps a DBTX to provide typed query methods for the kv table.
type kvStore struct{ db sqlflow.DBTX }

// newKVStore is a sqlflow.Querier: sqlflow calls it with the transaction's
// connection so every method on kvStore automatically runs within that
// transaction — no connection is ever passed around manually.
func newKVStore(db sqlflow.DBTX) *kvStore { return &kvStore{db: db} }

func (s *kvStore) Set(ctx context.Context, key, val string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO kv(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val`,
		key, val)

	return err
}

func (s *kvStore) Get(ctx context.Context, key string) (string, error) {
	var val string
	err := s.db.QueryRowContext(ctx, `SELECT val FROM kv WHERE key=?`, key).Scan(&val)

	return val, err
}

func ExampleOpenDB() {
	path := "/tmp/sqlflow_example.db"
	os.Remove(path)

	db, err := sqlflow.OpenDB(
		path,
		newKVStore,
		mattn.Driver,
		migrators.Goose(exampleMigrations),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := db.Write(ctx, func(s *kvStore) error {
		return s.Set(ctx, "hello", "world")
	}); err != nil {
		log.Fatal(err)
	}

	var val string

	if err := db.Read(ctx, func(s *kvStore) error {
		var err error
		val, err = s.Get(ctx, "hello")

		return err
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(val)
	// Output: world
}

func ExampleNewPool() {
	dir, err := os.MkdirTemp("", "sqlflow_pool_example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pool, err := sqlflow.NewPool(
		dir,
		newKVStore,
		1_000,
		mattn.Driver,
		migrators.Goose(exampleMigrations),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()

	if err := pool.Write(ctx, "alice", func(s *kvStore) error {
		return s.Set(ctx, "hello", "world")
	}); err != nil {
		log.Fatal(err)
	}

	var val string

	if err := pool.Read(ctx, "alice", func(s *kvStore) error {
		var err error
		val, err = s.Get(ctx, "hello")

		return err
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(val)
	// Output: world
}
