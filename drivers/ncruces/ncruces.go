// Package ncruces registers the github.com/ncruces/go-sqlite3 SQLite driver
// (WebAssembly, no CGo) and exports a ready-to-use [sqlflow.Option] that
// selects it.
//
// Import this package instead of importing the driver and calling WithDriver
// separately:
//
//	import (
//	    "github.com/avalonbits/sqlflow"
//	    "github.com/avalonbits/sqlflow/drivers/ncruces"
//	)
//
//	db, err := sqlflow.OpenDB(path, querier, ncruces.Driver, ...)
//
// This driver does not support at-rest encryption. Calling OpenEncryptedDB
// or NewEncryptedPool with this driver returns sqlflow.ErrEncryptionNotSupported.
//
// Note: this package embeds the SQLite WebAssembly binary via the
// github.com/ncruces/go-sqlite3/embed sub-package, which adds a few MB to
// your binary. If binary size is a concern, consider the mattn or modernc
// drivers instead.
package ncruces

import (
	"net/url"

	"github.com/avalonbits/sqlflow"
	"github.com/avalonbits/sqlflow/drivers"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

// Driver is the sqlflow option that registers and selects the
// ncruces/go-sqlite3 SQLite driver. Pass it to any sqlflow constructor.
var Driver = sqlflow.WithDriver(config)

var config = drivers.Config{
	Name:           "sqlite3",
	BuildDSN:       drivers.PragmaBuildDSN,
	BuildCipherDSN: nil,
	MemoryDSN: func(_, txlock string, params url.Values, pragmas [][2]string) string {
		return drivers.PragmaInMemoryDSN(false, ":memory:", txlock, params, pragmas)
	},
	IsPermanentErr: func(error) bool { return false },
}
