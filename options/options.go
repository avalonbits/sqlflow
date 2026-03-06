// Package options defines the functional option types shared between the sqlflow
// core and its migrator helpers. It depends only on the standard library so
// that neither sqlflow nor migrators packages create a circular import.
package options

import "database/sql"

// Config holds the per-DB lifecycle callbacks assembled from Option[Q] values.
// Multiple OnOpen / OnClose options registered on the same DB are chained in
// registration order.
type Config[Q any] struct {
	// OnOpenFn is called with the database file path and the live write
	// connection after all connections are established. A non-nil return aborts
	// the open and closes the connections.
	OnOpenFn func(path string, db *sql.DB) error

	// OnCloseFn is called exactly once when the DB is closed.
	OnCloseFn func()
}

// Option is a functional option that configures a DB instance.
type Option[Q any] func(*Config[Q])

// OnOpen registers fn to be called with the database file path and write
// connection once all connections are established and the DB is ready for use.
// If fn returns a non-nil error, the connections are closed and the error is
// propagated from the constructor.
//
// Multiple OnOpen options chain: each fn runs in registration order. For
// in-memory databases created by TestDB the path is ":memory:".
func OnOpen[Q any](fn func(path string, db *sql.DB) error) Option[Q] {
	return func(c *Config[Q]) {
		if c.OnOpenFn == nil {
			c.OnOpenFn = fn

			return
		}

		prev := c.OnOpenFn
		c.OnOpenFn = func(path string, db *sql.DB) error {
			if err := prev(path, db); err != nil {
				return err
			}

			return fn(path, db)
		}
	}
}

// OnClose registers fn to be called after both database connections are
// closed. Multiple OnClose options chain in registration order.
// fn fires at most once even if Close is called multiple times.
func OnClose[Q any](fn func()) Option[Q] {
	return func(c *Config[Q]) {
		if c.OnCloseFn == nil {
			c.OnCloseFn = fn

			return
		}

		prev := c.OnCloseFn
		c.OnCloseFn = func() {
			prev()
			fn()
		}
	}
}

// PoolConfig holds pool-level options assembled from PoolOption[Q] values.
type PoolConfig[Q any] struct {
	// DBFactory is called once per new pool entry to produce a fresh,
	// independent set of DB options.
	DBFactory func() []Option[Q]
}

// PoolOption is a functional option that configures a Pool instance at
// construction time.
type PoolOption[Q any] func(*PoolConfig[Q])

// WithDBFactory registers a factory that the Pool calls once for each new
// database entry to produce a fresh, independent set of DB options. Use a
// factory (rather than a fixed []Option[Q]) so that each opened database gets
// its own closure state (e.g. its own OS lock-file handle or migrator).
//
// The factory must return new closures on every invocation; sharing closure
// state across factory calls will cause data races.
func WithDBFactory[Q any](factory func() []Option[Q]) PoolOption[Q] {
	return func(c *PoolConfig[Q]) { c.DBFactory = factory }
}
