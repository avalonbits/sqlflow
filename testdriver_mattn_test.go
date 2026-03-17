//go:build !modernc && !ncruces

package sqlflow_test

import "github.com/avalonbits/sqlflow/drivers/mattn"

var (
	testDriver                   = mattn.Driver
	testDriverSupportsEncryption = true
)
