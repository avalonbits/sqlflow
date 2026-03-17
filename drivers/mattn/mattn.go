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
	"github.com/avalonbits/sqlflow"
	"github.com/avalonbits/sqlflow/drivers"

	_ "github.com/mattn/go-sqlite3"
)

// Driver is the sqlflow option that registers and selects the
// mattn/go-sqlite3 SQLite driver. Pass it to any sqlflow constructor.
var Driver = sqlflow.WithDriver(drivers.Mattn)
