# Global Preferences

- **Always commit after making changes** — don't wait for the user to ask. Commit immediately after a successful change + test pass.
- **Go: run `goimports`** on all updated/created `.go` files before committing changes to git.
    - If the go version is 1.26 or later, always run `go fix ./...` before running goimports.
- **Go tests: always call `t.Parallel()`** as the first line of every test function (and subtests) to ensure tests run in parallel.
- Always add a "Co-Authored-By: ModelName ModelVersion <email@model.owner>" line at the end of the commit. ModelName,
  ModelVersion and <email@model.owner> are templates for you to replace with the name of the model, version of the model and email of the
  owner of the model. Examples:
    - If the model is Kimi K2.5, the line should be: Co-Authored-By: Kimi K2.5 <info@moonshot.cn>
    - If the model is Claude Sonnet 4.6, the line should be: Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
# Project Memory

## Database / sqlc

- **Always use sqlc for queries**: Every SQL statement (SELECT, INSERT, UPDATE, DELETE) must be defined in the corresponding `server/storage/*store/queries.sql` file with a `-- name: QueryName :exec/:one/:many` annotation. Run `sqlc generate` after editing. Never write hand-rolled query functions in Go.
- **sqlc aggregate types**: Wrap COALESCE aggregate results with CAST so sqlc infers concrete Go types instead of `interface{}`. Use `CAST(COALESCE(SUM(col), 0) AS INTEGER)` → `int64`, `CAST(COALESCE(MAX(col), '') AS TEXT)` → `string`.

## Common Gotchas

- **Cookie values must be ASCII printable, no `"` `;` `\`** (RFC 6265). Go's `net/http` silently strips invalid bytes — JSON is unsafe as a raw cookie value. Always wrap JSON in `base64.RawURLEncoding` before storing in a cookie, and decode on read.

## Code Patterns

- **Slice building**: Use `make([]T, 0, len(source))` + `append` instead of `make([]T, len(source))` + index assignment.
- **API route naming**: Use singular resource names (e.g. `/api/pocket`, not `/api/pockets`).
- **Test assertions**: Always use `pretty.Diff` (`github.com/kr/pretty`) to compare structs/slices/data structures instead of checking individual fields.
- **Symbol ordering**: All exported types, functions, and methods must be defined before unexported ones, even if that places them far from usage. Package readers should see public API first, private details after.
- **Go import grouping**: Group imports as: (1) stdlib, (2) third-party, (3) internal — then **renamed/blank imports** (`_`, `alias "pkg"`) go in a final group at the bottom, separated by a blank line.

## Testing Requirements

- **Pre-commit sequence** (always in this order): `swag init -g cmd/trackm/main.go -o docs --parseDependency` → `sqlc generate` → `go fix ./...` → `goimports -w <changed .go files>` → `go test ./... -race` → **update architecture docs if needed** → commit. Fix any failures before committing.
- **Endpoint tests**: When adding or updating `/api/` endpoints, always add/update tests in **both** `cmd/trackm/trackm_test.go` (e2e with real HTTP) and `server/endpoints/api/web_test.go` (unit-level with `doJSON`).

## Live Server

- **Always keep a live server running** on `http://localhost:7080`. After every commit, rebuild and restart:
  ```
  go build -o trackm ./cmd/trackm/ && ./start.sh
  ```
  `start.sh` kills any process on port 7080, loads `.env`, and starts `./trackm` in the background.
- **NEVER delete the database files** when restarting the server. Only delete them if the user explicitly asks.
- DB files live under `/tmp/trackm/` (session DB, budget pool dir, audit pool dir) — defaults changed in `config/config.go`.
- `GET /healthz` → `"trackm"` confirms the server is up.
- **Web UI** is the SSR multi-page app served under `/web/*` (HTMX + PicoCSS). No SPA or separate client-side app exists or is planned.

## Secrets / Environment

- **Never commit `.env` or `.private_key`** — both are in `.gitignore`.
- `.env` contains `TRACKM_PEPPER` and `TRACKM_RECOVERY_PUBLIC_KEY`.
- `.private_key` (chmod 600) contains `TRACKM_RECOVERY_PRIVATE_KEY`; keep cold, only load when admin reset is needed.
- Keys were generated with `./trackm keygen`; regenerate only when rotating.

## Architecture Decisions

- **Recurring expenses/incomes**: Using Option A (compute at query time). If performance becomes an issue, see [Option B](recurring-option-b.md) for pre-materialization approach.
- **Swagger docs**: Generated with `swaggo/swag`. Every endpoint addition/update must also update swag annotations in `endpoints/api/api.go`. Regenerated as first step of every pre-commit sequence. Served at `GET /docs/api`.
- **Architecture docs**: Four docs must be kept in sync with the code. Update the relevant one(s) before every commit whenever the underlying architecture changes:
  - `docs/ARCHITECTURE.md` — overview, system diagram, auth model, data isolation, cross-cutting conventions
  - `server/endpoints/api/ARCHITECTURE.md` — REST API server: packages, layers, storage, services, middleware, handlers, config, testing
  - `client/command/ARCHITECTURE.md` — CLI client: command tree, auth subsystem, request helpers, all commands with flags
  - `server/endpoints/web/ARCHITECTURE.md` — Web UI: routes, cookie auth, CSRF, templates, HTMX pattern, UI components
  Triggers: new packages, new/changed routes, new middleware, new services, new storage types, new CLI commands, changed auth or session behaviour, changed config vars, changed UI pages or components.
- **README index**: `README.md` has a "Table of contents" section listing all `## ` headings. Always keep it in sync when adding, removing, or renaming sections.

## Project Go Styling Guide

We follow standard [Effective Go](https://go.dev/doc/effective_go) practices.

### Blank Lines

- Use a single blank line to separate logical blocks of code for improved readability.

Good example:
```go
func processData(data []string) error {
    // Initialization block
    if len(data) == 0 {
        return errors.New("no data provided")
    }

    // Processing block
    results, err := performProcessing(data)
    if err != nil {
        return err
    }

    // Finalization block
    fmt.Printf("Processed %d results\n", len(results))
    return nil
}
```

- Avoid as much as possible dense code.

Example to avoid:

```go
func processData(data []string) error {
    if len(data) == 0 {
        return errors.New("no data provided")
    }
    results, err := performProcessing(data)
    if err != nil {
        return err
    }
    fmt.Printf("Processed %d results\n", len(results))
    return nil
}
```

- Always add a blank line before a `return` statement, with the exception if it's the only statement of the scope.

Examples:

```go
func doSomething() error {
    callCode()

    return nil  // Blank line before return: OK
}
```

```go
func doSomething() error {
    callCode()
    return nil  // No blank line: bad
}
```


```go
func doSomething() error {
    test := callCode()
    if !test {
        return fmt.Errorf("test failed") // single return in scode: OK no blank line
    }

    return nil
}
```

```go
func doSomething() error {
    test := callCode()
    if !test {
        return fmt.Errorf("test failed") // single return in scode: OK no blank line
    }

    // This is a comment, it doesn't count as a statement, so no blank lines between it and return.
    return nil
}
```
