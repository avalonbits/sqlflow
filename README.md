# sqlflow

A SQLite-backed storage layer for Go. It wraps SQLite in WAL mode using the [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3)
driver, with separate read/write connections, serialised writes with exponential-backoff
retries, and an optional per-key connection pool backed by a [Ristretto](https://github.com/dgraph-io/ristretto) cache.

At-rest encryption is supported via SQLCipher.

All database access goes through `Read` and `Write` methods, which manage the transaction
for you, so you never touch a raw connection directly.

This package works nicely with [sqlc.dev](https://sqlc.dev), which creates named
queries as methods to a type that wrap database/sql.{DB,Tx} connections.

## Installation

```sh
go get github.com/avalonbits/sqlflow
```

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
        // A variadic list of options (see section on Options).
        // - migrators.Goose will apply migrations to the database using goose.
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
    dir,     // directory where per-user .db files are stored
    newKV,   // same Querier factory as OpenDB
    1_000,   // max cached open databases
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

To enable it, replace the standard `go-sqlite3` driver with the
[jgiannuzzi/go-sqlite3](https://github.com/jgiannuzzi/go-sqlite3) fork in your
`go.mod`:

```
replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806
```

Then use `OpenEncryptedDB` (single database) or pass a `keyProvider` to `NewEncryptedPool`
(per-key pool). Both accept a 32-byte key; sqlflow passes it to the driver via DSN parameters at open time.

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
db, err := sqlflow.OpenDB(path, mypackage.New, migrators.Goose(fsys))
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
error, keeping test setup concise:

```go
db   := sqlflow.TestDB(querier, migrators.Goose(fsys))
pool := sqlflow.TestPool(t.TempDir(), querier, migrators.Goose(fsys))
```


## License

MIT — see [LICENSE](LICENSE).
