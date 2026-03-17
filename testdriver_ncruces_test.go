//go:build ncruces

package sqlflow_test

import (
	"github.com/avalonbits/sqlflow"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

var testDriver = sqlflow.NcrucesDriver
var testDriverSupportsEncryption = false
