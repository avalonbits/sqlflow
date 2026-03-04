module github.com/avalonbits/sqlflow

go 1.26

require (
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/dgraph-io/ristretto/v2 v2.4.0
	github.com/mattn/go-sqlite3 v1.14.33
	github.com/pressly/goose/v3 v3.26.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
)

replace github.com/mattn/go-sqlite3 => github.com/jgiannuzzi/go-sqlite3 v1.14.35-0.20260227142656-2c447b9a2806
