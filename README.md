# sqlflow

A SQLite-backed storage layer for Go. It wraps SQLite in WAL mode with separate read/write
connections, serialised writes with exponential-backoff retries, and an optional per-key
connection pool backed by a [Ristretto](https://github.com/dgraph-io/ristretto) cache.

sqlflow is driver-agnostic: it imports only `database/sql` and works with any SQLite driver
you choose. At-rest encryption is supported via SQLCipher when using the mattn driver.

All database access goes through `Read` and `Write` methods, which manage the transaction
for you, so you never touch a raw connection directly.

This package works nicely with [sqlc.dev](https://sqlc.dev), which creates named
queries as methods to a type that wraps `database/sql.{DB,Tx}` connections.

## Installation

```sh
go get github.com/avalonbits/sqlflow
```

Then pick a driver sub-package (see [Driver selection](#driver-selection) below).

## Driver selection

sqlflow ships three driver sub-packages. Import the one you want — it registers
the underlying SQLite driver **and** provides the ready-to-use `Option` in a
single import, with no separate blank-import or `WithDriver` call needed:

| Sub-package | Driver | CGo | Encryption |
|-------------|--------|-----|------------|
| `github.com/avalonbits/sqlflow/drivers/mattn` | [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) | yes | yes (SQLCipher fork) |
| `github.com/avalonbits/sqlflow/drivers/modernc` | [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) | no | no |
| `github.com/avalonbits/sqlflow/drivers/ncruces` | [ncruces/go-sqlite3](https://github.com/ncruces/go-sqlite3) | no | no |

```go
// mattn (CGo, supports encryption):
import "github.com/avalonbits/sqlflow/drivers/mattn"
db, err := sqlflow.OpenDB(path, querier, mattn.Driver, ...)

// modernc (pure Go):
import "github.com/avalonbits/sqlflow/drivers/modernc"
db, err := sqlflow.OpenDB(path, querier, modernc.Driver, ...)

// ncruces (WebAssembly):
import "github.com/avalonbits/sqlflow/drivers/ncruces"
db, err := sqlflow.OpenDB(path, querier, ncruces.Driver, ...)
```

> [!NOTE]
> mattn and ncruces both register as `"sqlite3"` — they cannot coexist in the same binary.
> modernc registers as `"sqlite"` and can coexist with ncruces.

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing/fstest"

	"github.com/avalonbits/sqlflow"
	"github.com/avalonbits/sqlflow/drivers/mattn" // or modernc / ncruces
	"github.com/avalonbits/sqlflow/migrators"
)

func main() {
	path := "/tmp/plain.db"
	os.Remove(path)

	db, err := sqlflow.OpenDB(
		// the path to your database file.
		path,
		// A Querier function — sqlflow calls it with the open transaction.
		newKV,
		// Select the driver.
		mattn.Driver,
		// migrators.Goose applies migrations on open.
		migrators.Goose(migrations),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Write starts an immediate (exclusive) transaction and runs your func
	// inside it. Blocks if another write is in progress.
	err = db.Write(ctx, func(s *kvStore) error {
		return s.Set(ctx, "hello", "world")
	})
	if err != nil {
		log.Fatal(err)
	}

	var val string

	// Read starts a deferred transaction and can run concurrently with other
	// Read calls (but not with a Write).
	err = db.Read(ctx, func(s *kvStore) error {
		var err error
		val, err = s.Get(ctx, "hello")
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(val) // world
}

// migrations is an in-memory goose migration set. In production use
// //go:embed with fs.Sub, or os.DirFS, to point at real .sql files.
var migrations = fstest.MapFS{
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

In the typical case where you are working with a single database file, calling `sqlflow.OpenDB` with the
path and Querier factory is analogous to `sql.Open(...)` with an extra factory function.

> [!NOTE]
> Pass the plain file path — not a DSN URI. Paths containing `file:` or `?` are rejected with an
> error. Use `WithDSNParams` or `WithPragma` to set connection parameters
> (see [Connection parameters](#connection-parameters)).

`sqlflow.DB` is generic over your Querier type — `DB[kvStore]` in this example — which is why the
closure passed to `Read` and `Write` receives a concrete `*kvStore` rather than an interface. The
type is fixed at construction time via the factory function, so no type assertions are needed.

From then on you call `db.Read(ctx, func(q *kvStore) error { ... })` with your database code inside
the querier closure for concurrent read operations or use Write when you want to perform write operations.

While there is nothing preventing you from doing write operations within the Read closure, you should
avoid it: the read connection uses deferred transactions, so a write inside a Read closure can conflict
with an active Write and return SQLITE_BUSY. All Write calls are serialized with each other — only one
runs at a time — but concurrent Reads are always allowed, even while a Write is in progress.

## Pool usage

When each user (or tenant) needs their own isolated database file, use `NewPool` instead of `OpenDB`.
The pool opens databases lazily on first access and keeps them in a [Ristretto](https://github.com/dgraph-io/ristretto) cache.
Call `SetInactivityTimeout` to start a background reaper that evicts databases that idle longer than the given duration.

Options work exactly the same way as with `OpenDB` — pass them as the trailing variadic arguments.
The options are applied to every database the pool opens, so `migrators.Goose(fsys)` will run
migrations on each user's database the first time it is accessed.

```go
pool, err := sqlflow.NewPool(
    dir,                         // directory where per-user .db files are stored
    newKV,                       // same Querier factory as OpenDB
    1_000,                       // max cached open databases
    mattn.Driver,                // select driver
    migrators.Goose(migrations), // options — same as OpenDB
)
if err != nil {
    log.Fatal(err)
}
pool.SetInactivityTimeout(5 * time.Minute) // evict after 5 min idle
defer pool.Close()

ctx := context.Background()

// Read and Write take an extra key argument that selects the database.
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

fmt.Println(val) // world
```

The only difference from the single-database case is the `key` argument (`"alice"` above). Everything
else — the Querier type, the closure shape, the Read/Write semantics — is identical.

## Encryption

sqlflow supports at-rest encryption through [SQLCipher](https://www.zetetic.net/sqlcipher/),
a SQLite extension that encrypts the entire database file with AES-256.

Encryption requires `mattn.Driver` and the [jgiannuzzi/go-sqlite3](https://github.com/jgiannuzzi/go-sqlite3)
fork (which bundles SQLCipher). Add the replace directive to your `go.mod`:

```
replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806
```

Then use `OpenEncryptedDB` (single database) or `NewEncryptedPool` (per-key pool). Both accept a
32-byte key; sqlflow passes it to the driver via DSN parameters at open time.

```go
import "github.com/avalonbits/sqlflow/drivers/mattn" // must use the jgiannuzzi fork

key := make([]byte, 32) // fill with your 32-byte key

db, err := sqlflow.OpenEncryptedDB(
    path, querier, key,
    mattn.Driver,
    migrators.Goose(fsys),
)
```

Calling `OpenEncryptedDB` or `NewEncryptedPool` with `modernc.Driver` or `ncruces.Driver` returns
`sqlflow.ErrEncryptionNotSupported` immediately.

## Using with sqlc

[sqlc](https://sqlc.dev/) generates type-safe Go query functions from SQL. It's a natural fit for
sqlflow: sqlc produces a `New(db DBTX) *Queries` constructor and a `DBTX` interface that sqlflow
accepts directly, so there is no adapter code to write.

### Configure sqlc

A minimal `sqlc.yaml` for a SQLite project:

```yaml
version: "2"
sql:
  - engine: "sqlite"
    queries: "queries.sql"
    schema:  "migrations/"
    gen:
      go:
        package:     "store"
        out:         "store"
        sql_package: "database/sql"
```

> [!NOTE]
> Set `sql_package: "database/sql"` so sqlc generates a `DBTX` interface backed by the standard
> library — this is what sqlflow's `DBTX` is compatible with.

### Write your queries

```sql
-- queries.sql

-- name: GetUser :one
SELECT id, name FROM users WHERE id = ?;

-- name: CreateUser :exec
INSERT INTO users (id, name) VALUES (?, ?);
```

Run `sqlc generate` after editing `.sql` files to keep the generated code in sync.

### Wire it to sqlflow

Pass the generated `store.New` function directly as the `Querier` — sqlflow infers all type
parameters from it:

```go
import (
    "github.com/avalonbits/sqlflow"
    "github.com/avalonbits/sqlflow/drivers/mattn"
    "github.com/avalonbits/sqlflow/migrators"
    "myapp/store"
)

db, err := sqlflow.OpenDB(
    "/var/data/app.db",
    store.New,                   // sqlc-generated constructor, no wrapper needed
    mattn.Driver,
    migrators.Goose(migrationsFS),
)
```

`db` is a `*sqlflow.DB[store.Queries, store.DBTX]`. Inside `Read` and `Write` closures the
`*store.Queries` accessor gives you fully type-safe calls:

```go
err = db.Write(ctx, func(q *store.Queries) error {
    return q.CreateUser(ctx, store.CreateUserParams{ID: 1, Name: "Alice"})
})

err = db.Read(ctx, func(q *store.Queries) error {
    user, err := q.GetUser(ctx, 1)
    fmt.Println(user.Name) // Alice
    return err
})
```

## Concepts

### Read and Write

`Read` and `Write` are the core of sqlflow. The intention is that every
database interaction goes through one of them, keeping all query execution
inside a managed transaction. The API is designed to make correct transaction
handling the natural path: you never open a transaction manually, never call
commit or rollback, and never hold a raw connection.

```go
// DB
func (db *DB[Q, D])  Read (ctx context.Context,             f func(*Q) error) error
func (db *DB[Q, D])  Write(ctx context.Context,             f func(*Q) error) error

// Pool
func (p *Pool[Q, D]) Read (ctx context.Context, key string, f func(*Q) error) error
func (p *Pool[Q, D]) Write(ctx context.Context, key string, f func(*Q) error) error
```

Both methods accept a closure `f` that receives a `*Q` — your typed query
accessor — already bound to an open transaction. You call your query methods
on it; sqlflow commits on success or rolls back on any error, automatically,
with no extra code on your part.

> [!CAUTION]
> If your `Q` type has an exported field holding the underlying `DBTX`, it is
> technically possible to copy that value out of the closure and use it after
> `Read` or `Write` returns. You should not do this. The `DBTX` is bound to a
> transaction that sqlflow has already committed or rolled back by the time the
> closure exits; any query run against it afterwards will execute outside a
> transaction and with undefined behaviour — it may silently run against a stale
> connection, see an inconsistent snapshot, or fail with a driver error. Keep
> all database access inside the closure.

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

A `Querier[Q, D]` is a constructor function `func(tx D) *Q` that builds your
per-transaction accessor. `D` is the DBTX-compatible type your constructor
accepts — typically `sqlflow.DBTX` for hand-written code, or the
package-local `DBTX` generated by sqlc.

If you use [sqlc](https://sqlc.dev/), pass the generated `New` function
directly — no adapter wrapper is needed:

```go
// sqlc generates this in your queries package:
//
//   type DBTX interface { ExecContext(...) ... }
//   func New(db DBTX) *Queries { return &Queries{db: db} }
//
// Pass it straight to OpenDB — type parameters are inferred automatically:
db, err := sqlflow.OpenDB(path, mypackage.New, mattn.Driver, migrators.Goose(fsys))
```

For hand-written accessors, use `sqlflow.DBTX` directly:

```go
type Queries struct{ db sqlflow.DBTX }

func New(tx sqlflow.DBTX) *Queries { return &Queries{db: tx} }

var querier sqlflow.Querier[Queries, sqlflow.DBTX] = New
```

### Migrations

sqlflow has no built-in migration tool. Instead, migrations are applied through
the `OnOpen` hook: any option that runs schema changes against the live write
connection before the database is returned to the caller qualifies as a
migrator.

The `migrators` sub-package ships a ready-made [goose](https://github.com/pressly/goose)
integration. Pass `migrators.Goose(fsys)` as an option to any constructor
(`OpenDB`, `NewPool`, …) to run all pending goose migrations on open. The
`fs.FS` root must contain the `*.sql` files directly — no subdirectory.

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

To use a different migration tool, wrap it in `sqlflow.OnOpen`:

```go
sqlflow.OnOpen(func(path string, db *sql.DB) error {
    // run your migrations against db
    return myMigrator.Migrate(db)
})
```

### Options

All constructors (`OpenDB`, `OpenEncryptedDB`, `TestDB`, `NewPool`, `NewEncryptedPool`, `TestPool`)
accept a variadic `...Option` that configures the database:

| Option | Description |
|--------|-------------|
| `mattn.Driver` / `modernc.Driver` / `ncruces.Driver` | Select the SQLite driver (from the `drivers/` sub-packages). Required. |
| `WithDSNParams(params)` | Pass connection parameters using the mattn DSN query-string syntax. |
| `WithPragma(name, value)` | Set a SQLite PRAGMA on open, cross-driver. |
| `OnOpen(fn)` | Hook called with `(path string, db *sql.DB)` just after the database is opened. Errors abort the open. |
| `OnClose(fn)` | Hook called with `(path string, db *sql.DB)` just before the database is closed. |
| `migrators.Goose(fsys)` | Convenience `OnOpen` hook that runs goose migrations from `fsys`. |

### Connection parameters

> [!NOTE]
> Pass the file path directly to `OpenDB` — not a DSN URI. sqlflow builds the
> connection string internally. Passing `"file:/path/db?..."` or any path
> containing `?` returns an error.

Use `WithDSNParams` to pass connection parameters using the familiar mattn
query-string syntax. sqlflow translates them to the correct format for the
active driver automatically:

```go
db, err := sqlflow.OpenDB(
    "/var/data/app.db",
    querier,
    mattn.Driver,
    sqlflow.WithDSNParams("_foreign_keys=1&_cache_size=20000"),
)
```

For a single pragma, `WithPragma` is more explicit and works identically across
all three drivers:

```go
sqlflow.WithPragma("foreign_keys", "1")
sqlflow.WithPragma("cache_size", "20000")
```

**What sqlflow controls and you cannot override:**

| Parameter | Value | Reason |
|-----------|-------|--------|
| `_txlock` / `_pragma=locking_mode` | `immediate` (write), `deferred` (read) | Required for WAL correctness |
| `_journal` / `journal_mode` | `WAL` | Core guarantee of the library |

**Defaults you can override:**

| Parameter | Default | Override example |
|-----------|---------|-----------------|
| `_sync` / `synchronous` | `NORMAL` (1) | `WithDSNParams("_sync=2")` |
| `_busy_timeout` / `busy_timeout` | 5000 ms | `WithDSNParams("_busy_timeout=10000")` |
| `_cache_size` / `cache_size` | 10000 pages | `WithDSNParams("_cache_size=50000")` |

Any other `_`-prefixed parameter (e.g. `_foreign_keys`, `_auto_vacuum`) is
forwarded to the driver. For mattn it is passed as a flat query param; for
modernc and ncruces it is translated to `_pragma=name(value)` automatically,
so the same `WithDSNParams` call works across all drivers.

### Single database — `DB[Q]`

`OpenDB` creates the file and any parent directories, then opens separate read
and write connections in WAL mode. Pass `migrators.Goose(fsys)` to run
migrations on open.

### Per-key connection pool — `Pool[Q]`

`Pool` manages a collection of SQLite databases — one per key (e.g. one per
user). Databases are opened lazily and kept in a [Ristretto](https://github.com/dgraph-io/ristretto) cache; evicted
databases are closed only after all in-flight operations finish. Call
`SetInactivityTimeout` to enable background eviction of idle databases.

Use `NewEncryptedPool` to enable per-key encryption; it requires a `keyProvider`
function. If the key for a given user is unavailable, `Read`/`Write` return
`sqlflow.ErrKeyNotAvailable`.

### Testing

`TestDB` and `TestPool` create in-memory / temp-dir instances and panic on
error, keeping test setup concise. Pass the same options as the production
constructors:

```go
db   := sqlflow.TestDB(querier, mattn.Driver, migrators.Goose(fsys))
pool := sqlflow.TestPool(t.TempDir(), querier, mattn.Driver, migrators.Goose(fsys))
```

To run the same test suite against all three drivers, define a `testDriver`
variable in build-tag files — one per driver. The driver sub-packages make
this concise:

```go
// testdriver_mattn_test.go
//go:build !modernc && !ncruces
package mypackage_test

import "github.com/avalonbits/sqlflow/drivers/mattn"

var testDriver = mattn.Driver
```

```go
// testdriver_modernc_test.go
//go:build modernc
package mypackage_test

import "github.com/avalonbits/sqlflow/drivers/modernc"

var testDriver = modernc.Driver
```

```sh
go test ./...                  # mattn (default)
go test ./... -tags modernc    # modernc
go test ./... -tags ncruces    # ncruces
```

## License

MIT — see [LICENSE](LICENSE).
