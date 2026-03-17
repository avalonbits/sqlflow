// Package modernc registers the modernc.org/sqlite SQLite driver (pure Go,
// no CGo) and exports a ready-to-use [sqlflow.Option] that selects it.
//
// Import this package instead of importing the driver and calling WithDriver
// separately:
//
//	import (
//	    "github.com/avalonbits/sqlflow"
//	    "github.com/avalonbits/sqlflow/drivers/modernc"
//	)
//
//	db, err := sqlflow.OpenDB(path, querier, modernc.Driver, ...)
//
// This driver does not support at-rest encryption. Calling OpenEncryptedDB
// or NewEncryptedPool with this driver returns sqlflow.ErrEncryptionNotSupported.
package modernc

import (
	"net/url"

	"github.com/avalonbits/sqlflow"
	"github.com/avalonbits/sqlflow/drivers"

	_ "modernc.org/sqlite"
)

// Driver is the sqlflow option that registers and selects the modernc.org/sqlite
// SQLite driver. Pass it to any sqlflow constructor.
var Driver = sqlflow.WithDriver(config)

var config = drivers.Config{
	Name:           "sqlite",
	BuildDSN:       drivers.PragmaBuildDSN,
	BuildCipherDSN: nil,
	MemoryDSN: func(name, txlock string, params url.Values, pragmas [][2]string) string {
		return drivers.PragmaInMemoryDSN(true, name, txlock, params, pragmas)
	},
	IsPermanentErr: func(error) bool { return false },
}
