# sqlflow

A generic, SQLite-backed storage layer for Go. It wraps SQLite in WAL mode
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

The SQLCipher-encrypted variants require the jgiannuzzi fork. Add this to your
`go.mod`:

```
replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806
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
```

## Single database — `DB[Q]`

### Open / create

```go
db, err := sqlflow.GetDB("data/app.db", fsys, querier)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

`GetDB` creates the file and any parent directories, runs all pending goose
migrations, then opens separate read and write connections in WAL mode.

Use `OpenDB` on the hot path (e.g. after a server restart) to skip migrations
when the file already exists:

```go
db, err := sqlflow.OpenDB("data/app.db", fsys, querier)
```

### Reads

`Read` executes the callback inside a deferred read transaction. Multiple
goroutines may call `Read` concurrently.

```go
err := db.Read(ctx, func(q *Queries) error {
    row, err := q.GetUser(ctx, userID)
    if sqlflow.NoRows(err) {
        return fmt.Errorf("user not found")
    }
    return err
})
```

### Writes

`Write` executes the callback inside an immediate (exclusive) transaction.
Concurrent writers are serialised by an internal mutex. Transient busy errors
are retried with exponential backoff.

```go
err := db.Write(ctx, func(q *Queries) error {
    return q.InsertUser(ctx, InsertUserParams{
        ID:   userID,
        Name: name,
    })
})
```

### Encrypted database

```go
key := make([]byte, 32)
// ... populate key securely ...

db, err := sqlflow.GetEncryptedDB("data/secure.db", fsys, querier, key)
```

Use `OpenEncryptedDB` for the same skip-migration optimisation as `OpenDB`.

## Per-key connection pool — `Pool[Q]`

`Pool` manages a collection of SQLite databases — one per key (e.g. one per
user). Databases are opened lazily and kept in a TinyLFU cache; evicted
databases are closed only after all in-flight operations finish.

### Create a pool

```go
pool, err := sqlflow.NewPool(
    "data/users",   // directory; one <key>.db file per user
    fsys,
    querier,
    10_000,         // max cached open databases
    nil,            // keyProvider — nil for unencrypted pool
    30*time.Minute, // inactivity timeout; 0 to disable
)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()
```

### Read and write

```go
err := pool.Write(ctx, userID, func(q *Queries) error {
    return q.SaveNote(ctx, note)
})

err = pool.Read(ctx, userID, func(q *Queries) error {
    notes, err = q.ListNotes(ctx)
    return err
})
```

### Encrypted pool

Provide a `keyProvider` function that returns the per-user data key. Migration
is deferred until the key is available, so the pool can be created at startup
before any user authenticates.

```go
pool, err := sqlflow.NewPool(
    "data/users",
    fsys,
    querier,
    10_000,
    func(userID string) ([]byte, bool) {
        return keyStore.Get(userID) // returns (key, ok)
    },
    30*time.Minute,
)
```

If the key for a user is not in the store, `Read`/`Write` return
`sqlflow.ErrKeyNotAvailable`.

### Eviction

```go
// Immediately evict a user's database from the cache (closes lazily).
pool.Evict(userID)

// Block until all pending evictions have been processed.
pool.Wait()
```

## Testing

`TestDB` and `TestPool` create in-memory / temp-dir instances and panic on
error, keeping test setup concise.

```go
func TestMyFeature(t *testing.T) {
    t.Parallel()

    db := sqlflow.TestDB(fsys, querier)
    defer db.Close()

    ctx := context.Background()
    if err := db.Write(ctx, func(q *Queries) error {
        return q.InsertUser(ctx, ...)
    }); err != nil {
        t.Fatal(err)
    }
}

func TestMyPoolFeature(t *testing.T) {
    t.Parallel()

    pool := sqlflow.TestPool(t.TempDir(), fsys, querier)
    defer pool.Close()

    // ...
}
```

## WAL checkpoint

For long-running processes it can be useful to periodically checkpoint the WAL
back into the main database file:

```go
if err := db.Checkpoint(ctx); err != nil {
    log.Printf("checkpoint: %v", err)
}
```

## License

MIT — see [LICENSE](LICENSE).
