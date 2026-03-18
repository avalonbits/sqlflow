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

## Table of contents

- [Installation](#installation)
- [Contributing](#contributing)
- [Driver selection](#driver-selection)
- [Usage](#usage)
- [Pool usage](#pool-usage)
- [Encryption](#encryption)
- [Using with sqlc](#using-with-sqlc)
- [Concepts](#concepts)
- [License](#license)

## Installation

```sh
go get github.com/avalonbits/sqlflow
```

Then pick a driver sub-package (see [Driver selection](#driver-selection) below).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

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

See [CONCEPTS.md](CONCEPTS.md) for a detailed explanation of Read/Write,
Querier, Migrations, Options, Connection parameters, and Testing.

## License

MIT — see [LICENSE](LICENSE).
