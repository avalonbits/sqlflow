// Package drivers defines the [Config] type that sqlflow uses to build
// connection strings and detect permanent errors for a specific SQLite driver.
//
// Normally you do not use this package directly. Import one of the driver
// sub-packages instead — they register the underlying SQLite driver and
// export a ready-to-use sqlflow.Option:
//
//	import "github.com/avalonbits/sqlflow/drivers/mattn"
//
//	db, err := sqlflow.OpenDB(path, querier, mattn.Driver, ...)
//
// The pragma-style DSN helpers ([PragmaBuildDSN], [PragmaInMemoryDSN]) are
// exported for use by the modernc and ncruces driver sub-packages.
package drivers

import (
	"net/url"
	"strings"
)

// Config holds everything sqlflow needs to open connections for a specific
// SQLite driver. Construct one in the relevant drivers/* sub-package rather
// than here.
type Config struct {
	// Name is the driver name as registered with database/sql (e.g. "sqlite3").
	Name string

	// BuildDSN builds a plain (unencrypted) connection DSN for the given file
	// path and transaction lock mode.
	BuildDSN func(path, txlock string, params url.Values, pragmas [][2]string) string

	// BuildCipherDSN builds an encrypted connection DSN. Nil means the driver
	// does not support encryption; sqlflow returns ErrEncryptionNotSupported.
	BuildCipherDSN func(path, txlock string, key []byte, params url.Values, pragmas [][2]string) string

	// MemoryDSN builds an in-memory DSN for TestDB. name is a unique identifier
	// per TestDB call; drivers using shared-cache in-memory databases (modernc)
	// must use it to isolate parallel test instances.
	MemoryDSN func(name, txlock string, params url.Values, pragmas [][2]string) string

	// IsPermanentErr reports whether err is a connection-level error that should
	// never be retried (e.g. wrong cipher key, corrupt file).
	IsPermanentErr func(error) bool
}

// PragmaBuildDSN builds a plain file DSN using _pragma=name(value) query
// parameters. Used by the modernc and ncruces driver sub-packages.
func PragmaBuildDSN(path, txlock string, params url.Values, pragmas [][2]string) string {
	q := pragmaQueryValues(txlock, params, pragmas)

	return "file:" + path + "?" + q.Encode()
}

// PragmaInMemoryDSN builds an in-memory DSN for pragma-style drivers.
// name is the database name in the URI (unique per TestDB call for drivers that
// require shared-cache). shared adds cache=shared, which is required by modernc
// for in-memory databases opened across multiple connections.
func PragmaInMemoryDSN(shared bool, name, txlock string, params url.Values, pragmas [][2]string) string {
	q := pragmaQueryValues(txlock, params, pragmas)
	q.Set("mode", "memory")

	if shared {
		q.Set("cache", "shared")
	}

	return "file:" + name + "?" + q.Encode()
}

// pragmaQueryValues builds the shared url.Values for pragma-style DSNs used
// by the modernc and ncruces drivers. It applies defaults, user overrides, and
// locked params using the _pragma=name(value) query format.
func pragmaQueryValues(txlock string, userParams url.Values, pragmas [][2]string) url.Values {
	result := make(url.Values)

	// Collect user _pragma values, filtering the locked journal_mode.
	var userPragmaVals []string
	for _, pv := range userParams["_pragma"] {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(pv)), "journal_mode(") {
			continue
		}
		userPragmaVals = append(userPragmaVals, pv)
	}

	// Translate overridable flat params (_sync, _busy_timeout, _cache_size) from
	// WithDSNParams into pragma equivalents so they work for modernc/ncruces too.
	// These map mattn flat-param names to their SQLite pragma names.
	flatToPragma := [][2]string{
		{"_sync", "synchronous"},
		{"_busy_timeout", "busy_timeout"},
		{"_cache_size", "cache_size"},
	}
	for _, mapping := range flatToPragma {
		if v := userParams.Get(mapping[0]); v != "" {
			userPragmaVals = append(userPragmaVals, mapping[1]+"("+v+")")
		}
	}

	// Copy non-pragma, non-locked flat params from userParams.
	// Underscore-prefixed params (_name=value) are mattn-style pragma shortcuts;
	// translate them to pragma(value) format so they work on modernc/ncruces too.
	for k, vals := range userParams {
		switch k {
		case "_key", "_cipher", "_txlock", "_journal", "_sync", "_busy_timeout", "_cache_size":
			// Skip — locked, cipher-only, or already handled via flatToPragma above.
		case "_pragma":
			// Already collected above.
		default:
			if strings.HasPrefix(k, "_") {
				pragmaName := k[1:]
				for _, v := range vals {
					userPragmaVals = append(userPragmaVals, pragmaName+"("+v+")")
				}
			} else {
				result[k] = vals
			}
		}
	}

	// hasPragma reports whether name is already set by the user.
	hasPragma := func(name string) bool {
		prefix := strings.ToLower(name) + "("
		for _, pv := range userPragmaVals {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(pv)), prefix) {
				return true
			}
		}
		for _, p := range pragmas {
			if strings.EqualFold(p[0], name) {
				return true
			}
		}

		return false
	}

	// Add sqlflow defaults only if not already set by the caller.
	if !hasPragma("synchronous") {
		result.Add("_pragma", "synchronous(NORMAL)")
	}
	if !hasPragma("busy_timeout") {
		result.Add("_pragma", "busy_timeout(5000)")
	}
	if !hasPragma("cache_size") {
		result.Add("_pragma", "cache_size(10000)")
	}

	// Add user _pragma values (already filtered above).
	for _, pv := range userPragmaVals {
		result.Add("_pragma", pv)
	}

	// Add WithPragma pragmas, skipping the locked journal_mode.
	for _, p := range pragmas {
		if strings.EqualFold(p[0], "journal_mode") {
			continue
		}
		result.Add("_pragma", p[0]+"("+p[1]+")")
	}

	// Lock journal_mode and txlock — these always win.
	result.Add("_pragma", "journal_mode(WAL)")
	result.Set("_txlock", txlock)

	return result
}
