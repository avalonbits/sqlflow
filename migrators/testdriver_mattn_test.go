//go:build !modernc && !ncruces

package migrators_test

import (
	"github.com/avalonbits/sqlflow"

	_ "github.com/mattn/go-sqlite3"
)

var testDriver = sqlflow.MattnDriver
