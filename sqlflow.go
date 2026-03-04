// Package sqlflow provides a generic, SQLite-backed storage layer built on top
// of database/sql. It wraps SQLite in WAL mode with separate read and write
// connections, serialised writes, and exponential-backoff retry logic.
//
// The two main abstractions are:
//
//   - DB[Queries]: a single SQLite database whose per-transaction accessor is Queries.
//     Use GetDB or OpenDB to open an existing file, or TestDB for an
//     in-memory database in tests.
//
//   - Pool[Queries]: a per-key connection pool where each key (e.g. a user ID) maps
//     to its own SQLite file on disk. Connections are cached in a ristretto
//     TinyLFU cache and closed gracefully when evicted. Use NewPool to create
//     one, or TestPool in tests.
//
// Both types have encrypted variants: use GetEncryptedDB/OpenEncryptedDB and
// NewEncryptedPool instead of their plain counterparts. The jgiannuzzi fork of
// go-sqlite3 applies PRAGMA key via the DSN before any other pragmas.
//
// Migrations are handled by goose. Use EmbedMigrations to source them from an
// embedded FS, or DirMigrations to read them from a directory on disk.
package sqlflow

import (
	"context"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/pressly/goose/v3"

	sqlite3lib "github.com/mattn/go-sqlite3"
)

// Migrations specifies where goose migration files are located.
// Construct one with EmbedMigrations or DirMigrations.
type Migrations struct {
	// fsys is the filesystem to pass to goose.SetBaseFS.
	fsys fs.FS

	// dir is the directory path passed to goose.Up.
	// "migrations" for embedded FS layouts; "." for DirMigrations.
	dir string
}

// EmbedMigrations sources migration files from an embedded FS. The FS must
// contain a "migrations/" subdirectory holding the *.sql files.
func EmbedMigrations(fsys embed.FS) Migrations {
	return Migrations{fsys: fsys, dir: "migrations"}
}

// DirMigrations sources migration files directly from a directory on disk.
// path should point to the directory that contains the *.sql files.
func DirMigrations(path string) Migrations {
	return Migrations{fsys: os.DirFS(path), dir: "."}
}

// Querier is a function that builds a per-transaction accessor of type Queries from
// a DBTX. It is called once per transaction inside Read and Write.
type Querier[Queries any] func(tx DBTX) *Queries

// DBTX is the interface satisfied by both *sql.DB and *sql.Tx, allowing the
// same accessor type to be used within or outside a transaction.
type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// DB is a generic SQLite database handle parameterised by a per-transaction
// accessor type Queries. It maintains two underlying sql.DB connections:
//
//   - wrdb: a single write connection (MaxOpenConns=1) with _txlock=immediate,
//     serialised by a mutex so that only one writer can hold the SQLite WAL
//     write lock at a time.
//   - rddb: an unbounded pool of read connections with _txlock=deferred,
//     allowing concurrent readers to proceed without blocking writers.
//
// Every operation runs inside a transaction. Write calls retry on transient
// SQLite busy errors using exponential backoff; Read calls retry indefinitely
// until the context is cancelled.
type DB[Queries any] struct {
	// querier constructs the per-transaction accessor from a DBTX.
	querier Querier[Queries]

	// rddb is the read connection pool; uses _txlock=deferred to allow
	// concurrent readers.
	rddb *sql.DB

	// backoffRetries is the maximum number of write attempts before giving up.
	backoffRetries int

	// mu serialises access to wrdb so only one writer holds the SQLite WAL
	// write lock at a time.
	mu *sync.Mutex

	// wrdb is the single write connection; MaxOpenConns=1, _txlock=immediate.
	wrdb *sql.DB
}

// TestDB creates an in-memory SQLite database, runs migrations, and returns a
// DB ready for use in tests. Panics on any error so test setup stays concise.
func TestDB[Queries any](migrations Migrations, querier Querier[Queries]) *DB[Queries] {
	db, err := sql.Open("sqlite3", fmt.Sprintf(writeDSN, ":memory:"))
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1)

	if err := migrate(db, migrations); err != nil {
		db.Close()
		panic(err)
	}

	return &DB[Queries]{querier: querier, rddb: db, mu: &sync.Mutex{}, wrdb: db, backoffRetries: 1}
}

// GetDB opens (or creates) the SQLite database at dbName, runs all pending
// migrations, and returns an open DB.
func GetDB[Queries any](dbName string, migrations Migrations, querier Querier[Queries]) (*DB[Queries], error) {
	return getDB(dbName, migrations, querier, nil)
}

// GetEncryptedDB opens (or creates) the SQLCipher-encrypted SQLite database at
// dbName, runs all pending migrations, and returns an open DB.
func GetEncryptedDB[Queries any](dbName string, migrations Migrations, querier Querier[Queries], key []byte) (*DB[Queries], error) {
	return getDB(dbName, migrations, querier, key)
}

// OpenDB opens an existing database without running migrations. If the file
// does not exist yet, it falls back to GetDB (which creates and migrates it).
// Use this on the hot path when migrations have already been applied (e.g.
// via MigrateAll at startup).
func OpenDB[Queries any](dbName string, migrations Migrations, querier Querier[Queries]) (*DB[Queries], error) {
	if _, err := os.Stat(dbName); err != nil {
		// File doesn't exist — new DB, must create and migrate.
		return GetDB(dbName, migrations, querier)
	}

	return openDBConns(dbName, querier, nil)
}

// OpenEncryptedDB opens an existing SQLCipher-encrypted database without
// running migrations. If the file does not exist yet, it falls back to
// GetEncryptedDB (which creates and migrates it).
func OpenEncryptedDB[Queries any](dbName string, migrations Migrations, querier Querier[Queries], key []byte) (*DB[Queries], error) {
	if _, err := os.Stat(dbName); err != nil {
		// File doesn't exist — new DB, must create and migrate.
		return GetEncryptedDB(dbName, migrations, querier, key)
	}

	return openDBConns(dbName, querier, key)
}

func getDB[Queries any](dbName string, migrations Migrations, querier Querier[Queries], key []byte) (*DB[Queries], error) {
	if err := os.MkdirAll(filepath.Dir(dbName), 0o755); err != nil {
		return nil, fmt.Errorf("creating db dir: %w", err)
	}

	var db *sql.DB
	var err error

	if len(key) > 0 {
		db, err = sql.Open("sqlite3", cipherWriteDSNFor(dbName, key))
	} else {
		db, err = sql.Open("sqlite3", fmt.Sprintf(writeDSN, dbName))
	}
	if err != nil {
		return nil, err
	}

	if err := migrate(db, migrations); err != nil {
		db.Close()

		return nil, err
	}
	db.Close()

	return openDBConns(dbName, querier, key)
}

// Close closes both the read and write database connections. It waits for any
// in-flight operations to complete before returning.
func (db *DB[Queries]) Close() error {
	return errors.Join(db.rddb.Close(), db.wrdb.Close())
}

// Checkpoint runs PRAGMA wal_checkpoint(TRUNCATE) under the write mutex.
// WAL frames are moved into the main database file and, if all readers are
// done, the WAL file is reset to zero size.
func (db *DB[Queries]) Checkpoint(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.wrdb.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")

	return err
}

// Read executes f inside a read-only deferred transaction. It retries on
// transient SQLite busy errors using exponential backoff until ctx is
// cancelled. Errors returned by f are treated as permanent and not retried.
func (db *DB[Queries]) Read(ctx context.Context, f func(*Queries) error) error {
	return backoff.Retry(func() error {
		return db.transaction(ctx, db.rddb, f)
	}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
}

// Write executes f inside an immediate (exclusive) transaction under the write
// mutex. It retries on transient SQLite busy errors up to backoffRetries times
// with exponential backoff. Errors returned by f are treated as permanent and
// cause an immediate rollback with no retry.
func (db *DB[Queries]) Write(ctx context.Context, f func(*Queries) error) error {
	var err error

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	for range db.backoffRetries {
		db.mu.Lock()
		err = db.transaction(ctx, db.wrdb, f)
		db.mu.Unlock()

		if err == nil {
			return nil
		}

		var permanent *backoff.PermanentError
		if errors.As(err, &permanent) {
			return permanent.Err
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			return err
		}

		select {
		case <-time.After(next):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("error after exhasuting retries: %w", err)
}

// NoRows reports whether err is a sql.ErrNoRows "not found" result. Use this
// instead of errors.Is(err, sql.ErrNoRows) for readability at call sites.
func NoRows(err error) bool {
	return err != nil && errors.Is(err, sql.ErrNoRows)
}

// ErrKeyNotAvailable is returned by an encrypted Pool when the data key for a
// user is not in the in-memory key store (user not logged in, TTL expired, or
// server restarted). The caller should re-authenticate to reload the key.
var ErrKeyNotAvailable = errors.New("data key not available")

// Pool is a per-key connection pool backed by a ristretto cache with TinyLFU
// eviction. Each key (e.g. user ID) gets its own SQLite database file under
// dir. When the cache evicts an entry, its DB is closed only after all
// in-flight operations finish (reference-counted via poolEntry).
type Pool[Queries any] struct {
	// dir is the directory under which per-key *.db files are stored.
	dir string

	// migrations specifies where goose migration files are located.
	migrations Migrations

	// querier constructs the per-transaction accessor for each opened DB.
	querier Querier[Queries]

	// mu serialises DB creation so only one goroutine opens a new file at a
	// time (double-checked locking with the cache).
	mu sync.Mutex

	// cache is the ristretto TinyLFU cache mapping keys to open poolEntries.
	cache *ristretto.Cache[string, *poolEntry[Queries]]

	// keyProvider returns the SQLCipher key for a given pool key. Nil for
	// unencrypted pools.
	keyProvider func(userID string) ([]byte, bool)

	// inactivityTimeout is the idle duration after which a pool entry is
	// evicted by the background reaper. Zero disables the reaper.
	inactivityTimeout time.Duration

	// reapCancel stops the background inactivity reaper goroutine.
	reapCancel context.CancelFunc
}

// SetKeyProvider wires the data key lookup function into the pool. Must be
// called before any Read/Write on the pool.
func (p *Pool[Queries]) SetKeyProvider(fn func(string) ([]byte, bool)) {
	p.keyProvider = fn
}

// Evict immediately removes the pool entry for userID from the cache, closing
// the database once all in-flight operations finish. No-op if the entry is not
// cached.
func (p *Pool[Queries]) Evict(userID string) {
	p.cache.Del(userID)
}

// Wait blocks until all pending cache evictions have been processed.
func (p *Pool[Queries]) Wait() {
	p.cache.Wait()
}

// NewPool creates a Pool backed by on-disk SQLite databases. maxCached
// controls the maximum number of open databases kept in the cache (minimum
// 1000). inactivityTimeout, if > 0, starts a background reaper that evicts
// entries idle for longer than the timeout; pass 0 to disable.
// It ensures the directory exists and migrates all existing databases.
// keyProvider, if non-nil, is set on the pool before MigrateAll runs so that
// encrypted pools skip migration (per-DB migration is lazy in getOrCreate).
func NewPool[Queries any](
	dir string, migrations Migrations, querier Querier[Queries], maxCached int64,
	keyProvider func(string) ([]byte, bool),
	inactivityTimeout time.Duration,
) (*Pool[Queries], error) {
	maxCached = max(maxCached, 1000)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating pool dir: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Pool[Queries]{
		dir:               dir,
		migrations:        migrations,
		querier:           querier,
		keyProvider:       keyProvider,
		inactivityTimeout: inactivityTimeout,
		reapCancel:        cancel,
	}

	cache, err := newPoolCache[Queries](maxCached)
	if err != nil {
		cancel()

		return nil, fmt.Errorf("creating pool cache: %w", err)
	}
	p.cache = cache

	if err := p.MigrateAll(); err != nil {
		cancel()
		p.cache.Close()

		return nil, fmt.Errorf("migrating existing databases: %w", err)
	}

	if inactivityTimeout > 0 {
		go p.runInactivityReaper(ctx)
	}

	return p, nil
}

// TestPool returns a pool backed by dir for tests. Panics on error, matching
// the TestDB convention.
func TestPool[Queries any](dir string, migrations Migrations, querier Querier[Queries]) *Pool[Queries] {
	p, err := NewPool(dir, migrations, querier, 100_000, nil, 0)
	if err != nil {
		panic(fmt.Sprintf("creating test pool: %v", err))
	}

	return p
}

// Read acquires the database for key and executes f inside a read-only
// deferred transaction. The pool entry's reference count is held for the
// duration so the database is not closed while f is running.
func (p *Pool[Queries]) Read(ctx context.Context, key string, f func(*Queries) error) error {
	entry, err := p.acquire(key)
	if err != nil {
		return err
	}
	defer entry.release()

	return entry.db.Read(ctx, f)
}

// Write acquires the database for key and executes f inside an immediate
// (exclusive) transaction. The pool entry's reference count is held for the
// duration so the database is not closed while f is running.
func (p *Pool[Queries]) Write(ctx context.Context, key string, f func(*Queries) error) error {
	entry, err := p.acquire(key)
	if err != nil {
		return err
	}
	defer entry.release()

	return entry.db.Write(ctx, f)
}

// MigrateAll opens every *.db file under dir, runs migrations, and closes.
// If a keyProvider is configured, migration is skipped (lazy per-DB migration
// happens in getOrCreate when the data key is available).
func (p *Pool[Queries]) MigrateAll() error {
	if p.keyProvider != nil {
		return nil
	}

	matches, err := filepath.Glob(filepath.Join(p.dir, "*.db"))
	if err != nil {
		return fmt.Errorf("globbing db files: %w", err)
	}

	for _, path := range matches {
		db, err := sql.Open("sqlite3", fmt.Sprintf(writeDSN, path))
		if err != nil {
			return fmt.Errorf("opening %s for migration: %w", path, err)
		}
		if err := migrate(db, p.migrations); err != nil {
			db.Close()

			return fmt.Errorf("migrating %s: %w", path, err)
		}
		db.Close()
	}

	return nil
}

// ListKeys returns the key (user ID) for every database file in the pool
// directory. The returned slice is sorted by filesystem order.
func (p *Pool[Queries]) ListKeys() ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(p.dir, "*.db"))
	if err != nil {
		return nil, fmt.Errorf("listing pool keys: %w", err)
	}

	keys := make([]string, 0, len(matches))
	for _, path := range matches {
		key := strings.TrimSuffix(filepath.Base(path), ".db")
		keys = append(keys, key)
	}

	return keys, nil
}

// Close stops the inactivity reaper and closes all cached databases.
// sql.DB.Close waits for in-flight operations to finish, so this blocks until
// everything drains.
func (p *Pool[Queries]) Close() error {
	p.reapCancel()

	var errs []error

	p.cache.IterValues(func(entry *poolEntry[Queries]) (stop bool) {
		entry.closing.Store(true)
		entry.once.Do(func() {
			if err := entry.db.Close(); err != nil {
				errs = append(errs, err)
			}
		})

		return false
	})
	p.cache.Close()

	return errors.Join(errs...)
}

func openDBConns[Queries any](dbName string, querier Querier[Queries], key []byte) (*DB[Queries], error) {
	var rDSN, wDSN string

	if len(key) > 0 {
		wDSN = cipherWriteDSNFor(dbName, key)
		rDSN = cipherReadDSNFor(dbName, key)
	} else {
		wDSN = fmt.Sprintf(writeDSN, dbName)
		rDSN = fmt.Sprintf(readDSN, dbName)
	}

	wrdb, err := sql.Open("sqlite3", wDSN)
	if err != nil {
		return nil, err
	}
	wrdb.SetMaxOpenConns(1)

	rddb, err := sql.Open("sqlite3", rDSN)
	if err != nil {
		wrdb.Close()

		return nil, err
	}

	return &DB[Queries]{querier: querier, rddb: rddb, mu: &sync.Mutex{}, wrdb: wrdb, backoffRetries: 5}, nil
}

// runInactivityReaper periodically evicts pool entries that have been idle
// longer than p.inactivityTimeout. Stops when ctx is cancelled.
func (p *Pool[Queries]) runInactivityReaper(ctx context.Context) {
	interval := max(p.inactivityTimeout/4, time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.reapInactive()
		}
	}
}

func (p *Pool[Queries]) reapInactive() {
	cutoff := time.Now().Add(-p.inactivityTimeout).UnixNano()

	// Collect keys first; calling Del inside IterValues would deadlock because
	// IterValues holds a read lock and Del needs a write lock on the same shard.
	var toEvict []string
	p.cache.IterValues(func(entry *poolEntry[Queries]) (stop bool) {
		if entry.refs.Load() == 0 && entry.lastActivity.Load() < cutoff {
			toEvict = append(toEvict, entry.key)
		}

		return false
	})
	for _, key := range toEvict {
		p.cache.Del(key)
	}
}

// acquire returns a poolEntry with an incremented ref count. The caller must
// call release when done. If the entry is being evicted, acquire retries with
// a fresh entry.
func (p *Pool[Queries]) acquire(key string) (*poolEntry[Queries], error) {
	for {
		entry, err := p.getOrCreate(key)
		if err != nil {
			return nil, err
		}
		if entry.acquire() {
			entry.lastActivity.Store(time.Now().UnixNano())

			return entry, nil
		}
		// Entry is closing (evicted). Loop to create a replacement.
	}
}

func (p *Pool[Queries]) getOrCreate(key string) (*poolEntry[Queries], error) {
	if entry, ok := p.cache.Get(key); ok {
		return entry, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring lock.
	if entry, ok := p.cache.Get(key); ok {
		return entry, nil
	}

	dbPath := filepath.Join(p.dir, key+".db")

	var dbKey []byte
	if p.keyProvider != nil {
		k, ok := p.keyProvider(key)
		if !ok {
			return nil, fmt.Errorf("%w for user %q — please log in again", ErrKeyNotAvailable, key)
		}
		dbKey = k
	}

	newDB, err := getDB(dbPath, p.migrations, p.querier, dbKey)
	if err != nil {
		return nil, fmt.Errorf("opening db for %q: %w", key, err)
	}

	entry := &poolEntry[Queries]{
		key: key,
		db:  newDB,
	}
	entry.lastActivity.Store(time.Now().UnixNano())
	p.cache.Set(key, entry, 1)
	p.cache.Wait()

	return entry, nil
}

// poolEntry wraps a DB with reference counting so that evicted databases are
// not closed while goroutines are still using them.
type poolEntry[Queries any] struct {
	// key is the pool key (e.g. user ID) that identifies this entry.
	key string

	// db is the open database for this key.
	db *DB[Queries]

	// lastActivity records the last acquire time as unix nanoseconds; used by
	// the inactivity reaper to decide whether to evict the entry.
	lastActivity atomic.Int64

	// refs counts the number of active Read/Write callers holding this entry.
	refs atomic.Int32

	// closing is set to true when the entry has been evicted from the cache.
	// New acquires on a closing entry are rejected so a fresh entry is created.
	closing atomic.Bool

	// once ensures the database is closed exactly once regardless of how many
	// goroutines race to release or evict the entry.
	once sync.Once
}

// acquire increments the reference count. Returns false if the entry is being
// evicted, in which case the caller should obtain a fresh entry.
func (e *poolEntry[Queries]) acquire() bool {
	e.refs.Add(1)
	if e.closing.Load() {
		e.release()

		return false
	}

	return true
}

// release decrements the reference count. If the entry has been evicted and
// this is the last reference, it closes the database.
func (e *poolEntry[Queries]) release() {
	if e.refs.Add(-1) == 0 && e.closing.Load() {
		e.once.Do(func() {
			e.db.Close()
		})
	}
}

// evict marks the entry for closure. If no references are held, the database
// is closed immediately; otherwise the last release handles it.
func (e *poolEntry[Queries]) evict() {
	e.closing.Store(true)
	if e.refs.Load() == 0 {
		e.once.Do(func() {
			e.db.Close()
		})
	}
}

func newPoolCache[Queries any](maxCached int64) (*ristretto.Cache[string, *poolEntry[Queries]], error) {
	return ristretto.NewCache(&ristretto.Config[string, *poolEntry[Queries]]{
		NumCounters: maxCached * 10,
		MaxCost:     maxCached,
		BufferItems: 64,
		OnExit: func(entry *poolEntry[Queries]) {
			if entry != nil {
				entry.evict()
			}
		},
	})
}

var gooseMu sync.Mutex

const (
	readDSN  = "%s?_journal=wal&_sync=1&_busy_timeout=5000&_cache_size=10000&_txlock=deferred"
	writeDSN = "%s?_journal=wal&_sync=1&_busy_timeout=5000&_cache_size=10000&_txlock=immediate"
)

// cipherWriteDSNFor builds a DSN that applies PRAGMA key via the jgiannuzzi
// fork's native _key parameter. The fork executes PRAGMA key before any
// file-accessing pragmas (busy_timeout, synchronous, journal_mode), so WAL and
// synchronous settings work correctly on existing encrypted databases.
func cipherWriteDSNFor(path string, key []byte) string {
	return path + "?_key=x%27" + hex.EncodeToString(key) + "%27&_cipher=sqlcipher&_journal=wal&_sync=1&_busy_timeout=5000&_cache_size=10000&_txlock=immediate"
}

func cipherReadDSNFor(path string, key []byte) string {
	return path + "?_key=x%27" + hex.EncodeToString(key) + "%27&_cipher=sqlcipher&_journal=wal&_sync=1&_busy_timeout=5000&_cache_size=10000&_txlock=deferred"
}

func migrate(db *sql.DB, migrations Migrations) error {
	gooseMu.Lock()
	defer gooseMu.Unlock()

	goose.SetBaseFS(migrations.fsys)
	if err := goose.SetDialect("sqlite"); err != nil {
		return err
	}

	return goose.Up(db, migrations.dir)
}

func (db *DB[Queries]) transaction(ctx context.Context, rdbms *sql.DB, f func(*Queries) error) error {
	tx, err := rdbms.BeginTx(ctx, nil)
	if err != nil {
		// SQLITE_NOTADB ("file is not a database") means the cipher key is
		// wrong or the file is corrupt — retrying will never help.
		var sqliteErr sqlite3lib.Error
		if errors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3lib.ErrNotADB {
			return backoff.Permanent(fmt.Errorf("error creating transaction: %w", err))
		}

		return fmt.Errorf("error creating transaction: %w", err)
	}

	if err := f(db.querier(tx)); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return errors.Join(err, rbErr)
		}

		return backoff.Permanent(err)
	}

	return tx.Commit()
}
