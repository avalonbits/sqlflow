# sqlflow

A SQLite-backed storage layer for Go. It wraps SQLite in WAL mode
with separate read/write connections, serialised writes with exponential-backoff
retries, and an optional per-key connection pool backed by a TinyLFU cache.

Encryption is supported via the [jgiannuzzi/go-sqlite3](https://github.com/jgiannuzzi/go-sqlite3)
fork, which adds SQLCipher support through DSN parameters.

## Installation

```sh
go get github.com/avalonbits/sqlflow
```

Because sqlflow uses cgo (via go-sqlite3), you need a C compiler available at
build time.

## Examples

All four examples below are self-contained: copy any one into a `main.go`,
run `go mod init example && go mod tidy && go run .`, and it will compile and
run. They share the same boilerplate — a `kvStore` type backed by a simple
`kv(key, val)` table — and use `testing/fstest.MapFS` to supply migrations
in-memory without needing any files on disk.

### 1. Single database — plain

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing/fstest"

	"github.com/avalonbits/sqlflow"
)

var migrations = fstest.MapFS{
	"001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);
-- +goose Down
DROP TABLE kv;`)},
}

type kvStore struct{ db sqlflow.DBTX }

func newKV(db sqlflow.DBTX) *kvStore { return &kvStore{db: db} }

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

func main() {
	path := "/tmp/plain.db"
	os.Remove(path)

	db, err := sqlflow.GetDB(path, migrations, newKV)
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

	fmt.Println(val) // world
}
```

### 2. Single database — encrypted

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing/fstest"

	"github.com/avalonbits/sqlflow"
)

// go.mod must contain:
// replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806

var migrations = fstest.MapFS{
	"001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);
-- +goose Down
DROP TABLE kv;`)},
}

type kvStore struct{ db sqlflow.DBTX }

func newKV(db sqlflow.DBTX) *kvStore { return &kvStore{db: db} }

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

func main() {
	path := "/tmp/encrypted.db"
	os.Remove(path)

	// 32-byte key — in production load this from a secure secret store.
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}

	db, err := sqlflow.GetEncryptedDB(path, migrations, newKV, key)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := db.Write(ctx, func(s *kvStore) error {
		return s.Set(ctx, "secret", "value")
	}); err != nil {
		log.Fatal(err)
	}

	var val string
	if err := db.Read(ctx, func(s *kvStore) error {
		var err error
		val, err = s.Get(ctx, "secret")
		return err
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(val) // value
}
```

### 3. Connection pool — plain

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing/fstest"
	"time"

	"github.com/avalonbits/sqlflow"
)

var migrations = fstest.MapFS{
	"001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);
-- +goose Down
DROP TABLE kv;`)},
}

type kvStore struct{ db sqlflow.DBTX }

func newKV(db sqlflow.DBTX) *kvStore { return &kvStore{db: db} }

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

func main() {
	dir := "/tmp/pool-plain"
	os.RemoveAll(dir)

	pool, err := sqlflow.NewPool(
		dir,
		migrations,
		newKV,
		1_000,          // max cached open databases
		nil,            // no encryption
		5*time.Minute,  // evict after 5 min idle
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	users := []string{"alice", "bob", "carol"}

	// Each user gets their own isolated database file.
	for _, user := range users {
		user := user
		if err := pool.Write(ctx, user, func(s *kvStore) error {
			return s.Set(ctx, "greeting", "hello "+user)
		}); err != nil {
			log.Fatal(err)
		}
	}

	for _, user := range users {
		user := user
		var val string
		if err := pool.Read(ctx, user, func(s *kvStore) error {
			var err error
			val, err = s.Get(ctx, "greeting")
			return err
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println(user, "→", val)
	}
	// alice → hello alice
	// bob   → hello bob
	// carol → hello carol
}
```

### 4. Connection pool — encrypted

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing/fstest"
	"time"

	"github.com/avalonbits/sqlflow"
)

// go.mod must contain:
// replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806

var migrations = fstest.MapFS{
	"001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);
-- +goose Down
DROP TABLE kv;`)},
}

type kvStore struct{ db sqlflow.DBTX }

func newKV(db sqlflow.DBTX) *kvStore { return &kvStore{db: db} }

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

// keyStore simulates a session store that holds per-user encryption keys.
type keyStore struct {
	mu   sync.Mutex
	keys map[string][]byte
}

func (ks *keyStore) Set(userID string, key []byte) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.keys[userID] = key
}

func (ks *keyStore) Get(userID string) ([]byte, bool) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	k, ok := ks.keys[userID]
	return k, ok
}

func main() {
	dir := "/tmp/pool-encrypted"
	os.RemoveAll(dir)

	store := &keyStore{keys: make(map[string][]byte)}

	pool, err := sqlflow.NewPool(
		dir,
		migrations,
		newKV,
		1_000,
		store.Get,      // keyProvider — called per DB open
		5*time.Minute,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Simulate users logging in — each gets a unique 32-byte key.
	users := []string{"alice", "bob", "carol"}
	for i, user := range users {
		key := make([]byte, 32)
		for j := range key {
			key[j] = byte((i + 1) * (j + 1))
		}
		store.Set(user, key)
	}

	for _, user := range users {
		user := user
		if err := pool.Write(ctx, user, func(s *kvStore) error {
			return s.Set(ctx, "secret", "data for "+user)
		}); err != nil {
			log.Fatal(err)
		}
	}

	for _, user := range users {
		user := user
		var val string
		if err := pool.Read(ctx, user, func(s *kvStore) error {
			var err error
			val, err = s.Get(ctx, "secret")
			return err
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println(user, "→", val)
	}
	// alice → data for alice
	// bob   → data for bob
	// carol → data for carol
}
```

## Concepts

### Querier

A `Querier[Q]` is a constructor function `func(tx DBTX) *Q` that builds your
per-transaction accessor. If you use [sqlc](https://sqlc.dev/), pass
`db.New` directly; otherwise write a thin wrapper.

```go
type Queries struct{ db sqlflow.DBTX }

func New(tx sqlflow.DBTX) *Queries { return &Queries{db: tx} }

var querier sqlflow.Querier[Queries] = New
```

### Migrations

Pass an `fs.FS` whose **root** contains the goose `*.sql` migration files.

```go
// Embedded at compile time — sub-root so the FS root IS the migrations dir.
//go:embed migrations
var migrationsFS embed.FS

fsys, _ := fs.Sub(migrationsFS, "migrations")

// Or directly from disk at runtime:
fsys := os.DirFS("/path/to/migrations")

// Or in-memory for tests and examples:
fsys := fstest.MapFS{
    "001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE ...
-- +goose Down
DROP TABLE ...`)},
}
```

### Single database — `DB[Q]`

`GetDB` creates the file and any parent directories, runs all pending goose
migrations, then opens separate read and write connections in WAL mode.
Use `OpenDB` on the hot path to skip migrations when the file already exists.

`Read` executes its callback inside a deferred read transaction; multiple
goroutines may call it concurrently. `Write` executes inside an immediate
(exclusive) transaction serialised by an internal mutex, with exponential-backoff
retries on transient busy errors.

### Per-key connection pool — `Pool[Q]`

`Pool` manages a collection of SQLite databases — one per key (e.g. one per
user). Databases are opened lazily and kept in a TinyLFU cache; evicted
databases are closed only after all in-flight operations finish.

Supply a `keyProvider` function to enable per-key encryption. If the key for a
given user is unavailable, `Read`/`Write` return `sqlflow.ErrKeyNotAvailable`.

### Testing

`TestDB` and `TestPool` create in-memory / temp-dir instances and panic on
error, keeping test setup concise:

```go
db   := sqlflow.TestDB(fsys, querier)
pool := sqlflow.TestPool(t.TempDir(), fsys, querier)
```

## License

MIT — see [LICENSE](LICENSE).
