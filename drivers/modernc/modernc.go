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
	"github.com/avalonbits/sqlflow"

	_ "modernc.org/sqlite"
)

// Driver is the sqlflow option that registers and selects the modernc.org/sqlite
// SQLite driver. Pass it to any sqlflow constructor.
var Driver = sqlflow.WithDriver(sqlflow.ModerncDriver)
