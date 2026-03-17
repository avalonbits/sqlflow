//go:build modernc

package migrators_test

import (
	"github.com/avalonbits/sqlflow"

	_ "modernc.org/sqlite"
)

var testDriver = sqlflow.ModerncDriver
