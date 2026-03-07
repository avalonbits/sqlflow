// Package sqlflow provides a SQLite-backed storage layer built on top
// of database/sql. It wraps SQLite in WAL mode with separate read and write
// connections, serialised writes, and exponential-backoff retry logic.
//
// The two main abstractions are:
//
//   - DB[Queries]: a single SQLite database whose per-transaction accessor is Queries.
//     Use GetDB to open (or create) a file, or TestDB for an in-memory database
//     in tests.
//
//   - Pool[Queries]: a per-key connection pool where each key (e.g. a user ID) maps
//     to its own SQLite file on disk. Connections are cached in a ristretto
//     TinyLFU cache and closed gracefully when evicted. Use NewPool to create
//     one, or TestPool in tests.
//
// All database access goes through Read and Write methods, that manage the transaction for
// the callers.
//
// Both types have encrypted variants: use GetEncryptedDB and NewEncryptedPool
// instead of their plain counterparts. The jgiannuzzi fork of go-sqlite3
// applies PRAGMA key via the DSN before any other pragmas.
//
// Migrations are decoupled from the core: pass migrators.Goose(fsys) as an
// Option to run goose-based schema migrations on open, or implement your own
// OnOpen hook for any other migration tool.
package sqlflow

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dgraph-io/ristretto/v2"

	sqlite3lib "github.com/mattn/go-sqlite3"
)

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

// Option carries a single lifecycle hook for a DB instance. Construct one
// with OnOpen or OnClose; passing no options is always valid.
type Option struct {
	onOpen  func(path string, db *sql.DB) error
	onClose func()
}

// PoolOption carries a single pool-level configuration value. Construct one
// with WithDBFactory.
type PoolOption struct {
	dbFactory func() []Option
}

// OnOpen registers fn to be called with the database file path and the live
// write connection once all connections are established and the DB is ready for
// use. If fn returns a non-nil error, the connections are closed and the error
// is propagated from the constructor.
//
// Multiple OnOpen options run in registration order.
// For in-memory databases created by TestDB the path is ":memory:".
func OnOpen(fn func(path string, db *sql.DB) error) Option {
	return Option{onOpen: fn}
}

// OnClose registers fn to be called after both database connections are
// closed. Multiple OnClose options run in registration order.
// fn fires at most once even if Close is called multiple times. Use OnClose
// to release resources tied to this DB's lifetime (e.g. lock files).
func OnClose(fn func()) Option {
	return Option{onClose: fn}
}

// WithDBFactory registers a factory that the Pool calls once for each new
// database entry to produce a fresh, independent set of DB options. Use a
// factory (rather than a fixed []Option) so that each opened database gets
// its own closure state (e.g. its own OS lock-file handle or migrator).
//
// The factory must return new closures on every invocation; sharing closure
// state across factory calls will cause data races.
func WithDBFactory(factory func() []Option) PoolOption {
	return PoolOption{dbFactory: factory}
}

// DB is a SQLite database handle parameterised by a per-transaction accessor type
// Queries. It maintains two underlying sql.DB connections:
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

	// path is the database file path provided to the constructor; ":memory:"
	// for in-memory test databases.
	path string

	// onClose holds the registered close hooks, fired at most once on Close.
	onClose closeFnList

	// closeOnce ensures onClose fires at most once across multiple Close calls.
	closeOnce sync.Once
}

// TestDB creates an in-memory SQLite database and returns a DB ready for use
// in tests. Pass migrators.Goose(fsys) as an option to apply schema migrations.
//
// Panics on any error so test setup stays concise.
func TestDB[Queries any](querier Querier[Queries], opts ...Option) *DB[Queries] {
	conn, err := sql.Open("sqlite3", fmt.Sprintf(writeDSN, ":memory:"))
	if err != nil {
		panic(err)
	}
	conn.SetMaxOpenConns(1)

	onOpen, onClose := collectHooks(opts)

	if err := onOpen.run(":memory:", conn); err != nil {
		conn.Close()
		panic(fmt.Sprintf("onOpen hook: %v", err))
	}

	return &DB[Queries]{
		querier:        querier,
		rddb:           conn,
		mu:             &sync.Mutex{},
		wrdb:           conn,
		backoffRetries: 1,
		path:           ":memory:",
		onClose:        onClose,
	}
}

// GetDB opens (or creates) the SQLite database at dbName and returns an open DB.
// Pass migrators.Goose(fsys) as an option to run schema migrations.
func GetDB[Queries any](dbName string, querier Querier[Queries], opts ...Option) (*DB[Queries], error) {
	return getDB(dbName, querier, nil, opts)
}

// GetEncryptedDB opens (or creates) the SQLCipher-encrypted SQLite database at
// dbName and returns an open DB. Pass migrators.Goose(fsys) as an option to
// run schema migrations.
func GetEncryptedDB[Queries any](dbName string, querier Querier[Queries], key []byte, opts ...Option) (*DB[Queries], error) {
	return getDB(dbName, querier, key, opts)
}

// Close calls the OnClose hook (if any) exactly once, then closes both the
// read and write database connections. It waits for any in-flight operations
// to complete before returning.
func (db *DB[Queries]) Close() error {
	db.closeOnce.Do(db.onClose.run)

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
// user is not in the in-memory key store.
var ErrKeyNotAvailable = errors.New("data key not available")

// Pool is a per-key connection pool backed by a ristretto cache with TinyLFU
// eviction. Each key (e.g. user ID) gets its own SQLite database file under
// dir. When the cache evicts an entry, its DB is closed only after all
// in-flight operations finish (reference-counted via poolEntry).
type Pool[Queries any] struct {
	// dir is the directory under which per-key *.db files are stored.
	dir string

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

	// dbFactory, if non-nil, is called once per new pool entry to produce a
	// fresh set of DB options (e.g. per-DB migration hooks or OS lock-file
	// handles). Each call must return new closures with independent state.
	dbFactory func() []Option
}

// NewPool creates a plain (unencrypted) Pool backed by on-disk SQLite
// databases. maxCached controls the maximum number of open databases kept in
// the cache (minimum 1000). inactivityTimeout, if > 0, starts a background
// reaper that evicts entries idle for longer than the timeout; pass 0 to
// disable. Pass WithDBFactory(func() []Option{migrators.Goose(fsys)})
// to apply schema migrations on first open.
func NewPool[Queries any](
	dir string, querier Querier[Queries], maxCached int64,
	inactivityTimeout time.Duration, opts ...PoolOption,
) (*Pool[Queries], error) {
	return newPool(dir, querier, maxCached, nil, inactivityTimeout, opts)
}

// NewEncryptedPool creates a Pool where each database is encrypted with
// SQLCipher. keyProvider is called with the pool key (e.g. user ID) each time
// a database is opened; it must return the 32-byte encryption key and true, or
// false if the key is unavailable (causing Read/Write to return
// ErrKeyNotAvailable). Migration for encrypted databases is lazy: it runs on
// first open when the data key is available.
func NewEncryptedPool[Queries any](
	dir string, querier Querier[Queries], maxCached int64,
	keyProvider func(string) ([]byte, bool),
	inactivityTimeout time.Duration, opts ...PoolOption,
) (*Pool[Queries], error) {
	return newPool(dir, querier, maxCached, keyProvider, inactivityTimeout, opts)
}

// TestPool returns a plain pool backed by dir for tests. Panics on error,
// matching the TestDB convention.
func TestPool[Queries any](dir string, querier Querier[Queries], opts ...PoolOption) *Pool[Queries] {
	p, err := NewPool(dir, querier, 100_000, 0, opts...)
	if err != nil {
		panic(fmt.Sprintf("creating test pool: %v", err))
	}

	return p
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

type openFnList []func(path string, db *sql.DB) error

func (olf openFnList) run(path string, db *sql.DB) error {
	for _, fn := range olf {
		if err := fn(path, db); err != nil {
			return err
		}
	}

	return nil
}

type closeFnList []func()

func (clf closeFnList) run() {
	for _, fn := range clf {
		fn()
	}
}

func collectHooks(opts []Option) (openFnList, closeFnList) {
	var onOpen openFnList
	var onClose closeFnList

	for _, opt := range opts {
		if opt.onOpen != nil {
			onOpen = append(onOpen, opt.onOpen)
		}

		if opt.onClose != nil {
			onClose = append(onClose, opt.onClose)
		}
	}

	return onOpen, onClose
}

func getDB[Queries any](dbName string, querier Querier[Queries], key []byte, opts []Option) (*DB[Queries], error) {
	if err := os.MkdirAll(filepath.Dir(dbName), 0o755); err != nil {
		return nil, fmt.Errorf("creating db dir: %w", err)
	}

	return openDBConns(dbName, querier, key, opts)
}

func newPool[Queries any](
	dir string, querier Querier[Queries], maxCached int64,
	keyProvider func(string) ([]byte, bool),
	inactivityTimeout time.Duration,
	opts []PoolOption,
) (*Pool[Queries], error) {
	maxCached = max(maxCached, 1000)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating pool dir: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var dbFactory func() []Option
	for _, opt := range opts {
		if opt.dbFactory != nil {
			dbFactory = opt.dbFactory
		}
	}

	p := &Pool[Queries]{
		dir:               dir,
		querier:           querier,
		keyProvider:       keyProvider,
		inactivityTimeout: inactivityTimeout,
		reapCancel:        cancel,
		dbFactory:         dbFactory,
	}

	cache, err := newPoolCache[Queries](maxCached)
	if err != nil {
		cancel()

		return nil, fmt.Errorf("creating pool cache: %w", err)
	}
	p.cache = cache

	if inactivityTimeout > 0 {
		go p.runInactivityReaper(ctx)
	}

	return p, nil
}

func openDBConns[Queries any](dbName string, querier Querier[Queries], key []byte, opts []Option) (*DB[Queries], error) {
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

	onOpen, onClose := collectHooks(opts)

	if err := onOpen.run(dbName, wrdb); err != nil {
		rddb.Close()
		wrdb.Close()

		return nil, fmt.Errorf("onOpen hook for %q: %w", dbName, err)
	}

	return &DB[Queries]{
		querier:        querier,
		rddb:           rddb,
		mu:             &sync.Mutex{},
		wrdb:           wrdb,
		backoffRetries: 5,
		path:           dbName,
		onClose:        onClose,
	}, nil
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

	var dbOpts []Option
	if p.dbFactory != nil {
		dbOpts = p.dbFactory()
	}

	newDB, err := getDB(dbPath, p.querier, dbKey, dbOpts)
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

func (e *poolEntry[Queries]) acquire() bool {
	e.refs.Add(1)
	if e.closing.Load() {
		e.release()

		return false
	}

	return true
}

func (e *poolEntry[Queries]) release() {
	if e.refs.Add(-1) == 0 && e.closing.Load() {
		e.once.Do(func() {
			e.db.Close()
		})
	}
}

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

func (db *DB[Queries]) transaction(ctx context.Context, rdbms *sql.DB, f func(*Queries) error) error {
	tx, err := rdbms.BeginTx(ctx, nil)
	if err != nil {
		// "sql: database is closed" means Close was called — retrying is pointless.
		if strings.Contains(err.Error(), "database is closed") {
			return backoff.Permanent(fmt.Errorf("error creating transaction: %w", err))
		}

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
