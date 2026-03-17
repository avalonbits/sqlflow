//go:build !modernc && !ncruces

package sqlflow_test

import (
	"github.com/avalonbits/sqlflow"

	_ "github.com/mattn/go-sqlite3"
)

var testDriver = sqlflow.MattnDriver
var testDriverSupportsEncryption = true
