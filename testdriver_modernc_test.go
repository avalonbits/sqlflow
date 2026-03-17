//go:build modernc

package sqlflow_test

import "github.com/avalonbits/sqlflow/drivers/modernc"

var (
	testDriver                   = modernc.Driver
	testDriverSupportsEncryption = false
)
