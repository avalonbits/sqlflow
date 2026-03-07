// Package migrators provides migration helpers for sqlflow databases.
// Each helper returns a sqlflow.Option[Q] that can be passed to any
// sqlflow constructor (GetDB, TestDB, or via WithDBFactory for pools).
package migrators

import (
	"database/sql"
	"io/fs"
	"sync"

	"github.com/pressly/goose/v3"

	"github.com/avalonbits/sqlflow"
)

// Goose returns an Option that runs goose migrations from fsys when the
// database is opened. The migration runs on the live write connection so it
// works correctly for both plain and SQLCipher-encrypted databases.
//
// fsys must contain the *.sql migration files at its root (no subdirectory).
// Use embed.FS with fs.Sub or os.DirFS to obtain a suitable fs.FS.
func Goose(fsys fs.FS) sqlflow.Option {
	return sqlflow.OnOpen(func(_ string, db *sql.DB) error {
		return migrate(db, fsys)
	})
}

var gooseMu sync.Mutex

func migrate(db *sql.DB, fsys fs.FS) error {
	gooseMu.Lock()
	defer gooseMu.Unlock()

	goose.SetBaseFS(fsys)

	if err := goose.SetDialect("sqlite"); err != nil {
		return err
	}

	return goose.Up(db, ".")
}
