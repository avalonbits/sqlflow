//go:build modernc

package sqlflow_test

import (
	"github.com/avalonbits/sqlflow"

	_ "modernc.org/sqlite"
)

var testDriver = sqlflow.ModerncDriver
var testDriverSupportsEncryption = false
