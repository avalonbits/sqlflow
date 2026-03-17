// Package mattn registers the github.com/mattn/go-sqlite3 SQLite driver and
// exports a ready-to-use [sqlflow.Option] that selects it.
//
// Import this package instead of importing the driver and calling WithDriver
// separately:
//
//	import (
//	    "github.com/avalonbits/sqlflow"
//	    "github.com/avalonbits/sqlflow/drivers/mattn"
//	)
//
//	db, err := sqlflow.OpenDB(path, querier, mattn.Driver, ...)
//
// Encryption (SQLCipher) is supported when using the jgiannuzzi fork of
// go-sqlite3. Add this replace directive to your go.mod:
//
//	replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806
package mattn

import (
	"encoding/hex"
	"maps"
	"net/url"
	"strings"

	"github.com/avalonbits/sqlflow"
	"github.com/avalonbits/sqlflow/drivers"

	_ "github.com/mattn/go-sqlite3"
)

// Driver is the sqlflow option that registers and selects the
// mattn/go-sqlite3 SQLite driver. Pass it to any sqlflow constructor.
var Driver = sqlflow.WithDriver(config)

var config = drivers.Config{
	Name:           "sqlite3",
	BuildDSN:       buildDSN,
	BuildCipherDSN: buildCipherDSN,
	MemoryDSN: func(_, txlock string, params url.Values, pragmas [][2]string) string {
		return buildDSN(":memory:", txlock, params, pragmas)
	},
	IsPermanentErr: func(err error) bool {
		// SQLITE_NOTADB (error 26) — file is not a database or cipher key is wrong.
		return strings.Contains(err.Error(), "file is not a database")
	},
}

func buildDSN(path, txlock string, userParams url.Values, pragmas [][2]string) string {
	merged := make(url.Values, len(userParams)+6)
	maps.Copy(merged, userParams)

	// Strip cipher params — use OpenEncryptedDB for encrypted databases.
	delete(merged, "_key")
	delete(merged, "_cipher")

	// Apply sqlflow defaults for tuning params the caller did not set.
	if merged.Get("_sync") == "" {
		merged.Set("_sync", "1")
	}
	if merged.Get("_busy_timeout") == "" {
		merged.Set("_busy_timeout", "5000")
	}
	if merged.Get("_cache_size") == "" {
		merged.Set("_cache_size", "10000")
	}

	// Apply WithPragma pragmas as flat params, skipping locked ones.
	for _, p := range pragmas {
		if strings.EqualFold(p[0], "journal_mode") || strings.EqualFold(p[0], "txlock") {
			continue
		}
		merged.Set("_"+p[0], p[1])
	}

	// Lock correctness-critical params — these always win.
	merged.Set("_journal", "wal")
	merged.Set("_txlock", txlock)

	return path + "?" + merged.Encode()
}

func buildCipherDSN(path, txlock string, key []byte, userParams url.Values, pragmas [][2]string) string {
	merged := make(url.Values, len(userParams)+7)
	maps.Copy(merged, userParams)

	// Apply sqlflow defaults for tuning params the caller did not set.
	if merged.Get("_sync") == "" {
		merged.Set("_sync", "1")
	}
	if merged.Get("_busy_timeout") == "" {
		merged.Set("_busy_timeout", "5000")
	}
	if merged.Get("_cache_size") == "" {
		merged.Set("_cache_size", "10000")
	}

	// Apply WithPragma pragmas as flat params, skipping locked ones.
	for _, p := range pragmas {
		if strings.EqualFold(p[0], "journal_mode") || strings.EqualFold(p[0], "txlock") {
			continue
		}
		merged.Set("_"+p[0], p[1])
	}

	// Lock correctness-critical and cipher params — these always win.
	merged.Set("_journal", "wal")
	merged.Set("_txlock", txlock)
	merged.Set("_cipher", "sqlcipher")
	merged.Set("_key", "x'"+hex.EncodeToString(key)+"'")

	return path + "?" + merged.Encode()
}
