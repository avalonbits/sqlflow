# sqlflow

A SQLite-backed storage layer for Go. It wraps SQLite in WAL mode
with separate read/write connections, serialised writes with exponential-backoff
retries, and an optional per-key connection pool backed by a TinyLFU cache.
At-rest encryption is supported via SQLCipher.

All database access goes through `Read` and `Write` — the core abstraction.
They manage transactions automatically so you never touch a raw connection directly.

## Table of Contents

- [Installation](#installation)
- [Encryption](#encryption)
- [Concepts](#concepts)
  - [Read and Write](#read-and-write)
  - [Querier](#querier)
  - [Migrations](#migrations)
  - [Single database — DB\[Q\]](#single-database--dbq)
  - [Per-key connection pool — Pool\[Q\]](#per-key-connection-pool--poolq)
  - [Testing](#testing)
- [Examples](#examples)
  - [1. Single database — plain](#1-single-database--plain)
  - [2. Single database — encrypted](#2-single-database--encrypted)
  - [3. Connection pool — plain](#3-connection-pool--plain)
  - [4. Connection pool — encrypted](#4-connection-pool--encrypted)
- [License](#license)

## Installation

```sh
go get github.com/avalonbits/sqlflow
```

Because sqlflow uses cgo (via go-sqlite3), you need a C compiler available at
build time.

## Encryption

sqlflow supports at-rest encryption through [SQLCipher](https://www.zetetic.net/sqlcipher/),
a SQLite extension that encrypts the entire database file with AES-256.

To enable it, replace the standard `go-sqlite3` driver with the
[jgiannuzzi/go-sqlite3](https://github.com/jgiannuzzi/go-sqlite3) fork in your
`go.mod`:

```
replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806
```

Then use `GetEncryptedDB` / `OpenEncryptedDB` (single database) or pass a
`keyProvider` to `NewPool` (per-key pool). Both accept a 32-byte key; sqlflow
passes it to the driver via DSN parameters at open time.

## Concepts

### Read and Write

`Read` and `Write` are the core of sqlflow. Every database interaction goes
through one of them — there is no way to obtain a raw connection or run a
query outside a managed transaction. This is deliberate: the API makes
correct transaction handling the only path forward.

```go
// DB
func (db *DB[Q])  Read (ctx context.Context,             f func(*Q) error) error
func (db *DB[Q])  Write(ctx context.Context,             f func(*Q) error) error

// Pool
func (p *Pool[Q]) Read (ctx context.Context, key string, f func(*Q) error) error
func (p *Pool[Q]) Write(ctx context.Context, key string, f func(*Q) error) error
```

Both methods accept a closure `f` that receives a `*Q` — your typed query
accessor — already bound to an open transaction. You call your query methods
on it; sqlflow commits on success or rolls back on any error, automatically,
with no extra code on your part. You cannot accidentally run a query outside a
transaction, mix transactional and non-transactional calls, or forget to commit.

**Read** opens a deferred (read-only) transaction on a shared connection pool,
so multiple goroutines may call it concurrently without blocking each other.
Transient busy errors are retried with exponential backoff until `ctx` is
cancelled.

**Write** opens an immediate (exclusive) transaction on the single write
connection, serialised by an internal mutex so only one writer runs at a time
per database. Transient busy errors are retried up to five times with
exponential backoff. Errors returned by `f` are treated as permanent: the
transaction rolls back immediately with no retry, and the original error is
returned to the caller unchanged.

For `Pool`, the `key` argument (e.g. a user ID) selects which database to
operate on; everything else is identical.

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

sqlflow uses [goose](https://github.com/pressly/goose) for migrations. Every
open function (`GetDB`, `NewPool`, …) runs all pending migrations automatically
before returning. Migrations are supplied as an `fs.FS` whose root contains the
`*.sql` files directly — no subdirectory.

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

## Examples

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

// migrations is an in-memory goose migration set. In production use
// //go:embed with fs.Sub, or os.DirFS, to point at real .sql files.
var migrations = fstest.MapFS{
	"001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);
-- +goose Down
DROP TABLE kv;`)},
}

// kvStore wraps a DBTX to provide typed query methods for the kv table.
type kvStore struct{ db sqlflow.DBTX }

// newKV is a sqlflow.Querier: sqlflow calls it with the transaction's
// connection so every method on kvStore automatically runs within that
// transaction — no connection is ever passed around manually.
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
```

### 2. Single database — encrypted

```go
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"testing/fstest"

	"github.com/avalonbits/sqlflow"
)

// go.mod must contain:
// replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806

func main() {
	path := "/tmp/encrypted.db"
	os.Remove(path)

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		log.Fatal(err)
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

// migrations is an in-memory goose migration set. In production use
// //go:embed with fs.Sub, or os.DirFS, to point at real .sql files.
var migrations = fstest.MapFS{
	"001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);
-- +goose Down
DROP TABLE kv;`)},
}

// kvStore wraps a DBTX to provide typed query methods for the kv table.
type kvStore struct{ db sqlflow.DBTX }

// newKV is a sqlflow.Querier: sqlflow calls it with the transaction's
// connection so every method on kvStore automatically runs within that
// transaction — no connection is ever passed around manually.
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

// migrations is an in-memory goose migration set. In production use
// //go:embed with fs.Sub, or os.DirFS, to point at real .sql files.
var migrations = fstest.MapFS{
	"001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);
-- +goose Down
DROP TABLE kv;`)},
}

// kvStore wraps a DBTX to provide typed query methods for the kv table.
type kvStore struct{ db sqlflow.DBTX }

// newKV is a sqlflow.Querier: sqlflow calls it with the transaction's
// connection so every method on kvStore automatically runs within that
// transaction — no connection is ever passed around manually.
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
```

### 4. Connection pool — encrypted

```go
package main

import (
	"context"
	"crypto/rand"
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
	for _, user := range users {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			log.Fatal(err)
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

// migrations is an in-memory goose migration set. In production use
// //go:embed with fs.Sub, or os.DirFS, to point at real .sql files.
var migrations = fstest.MapFS{
	"001_init.sql": {Data: []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT NOT NULL);
-- +goose Down
DROP TABLE kv;`)},
}

// kvStore wraps a DBTX to provide typed query methods for the kv table.
type kvStore struct{ db sqlflow.DBTX }

// newKV is a sqlflow.Querier: sqlflow calls it with the transaction's
// connection so every method on kvStore automatically runs within that
// transaction — no connection is ever passed around manually.
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
```

## License

MIT — see [LICENSE](LICENSE).
